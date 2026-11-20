ENABLE_NTFY = True
# -*- coding: utf-8 -*-
"""
Toobit Crypto Pre-Move Hunter v3 — GitHub Actions
Public market data only. No API key. No order or trading endpoints.
Scans all eligible crypto USDT-M contracts by 24h ticker, then deeply analyzes
the top N liquid contracts. It is a research/watchlist tool, NOT a predictor.
"""
import time
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import statistics
import os
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------- CONFIG ----------------
BASE = "https://api.toobit.com"
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "360"))
RUN_ONCE = os.getenv("RUN_ONCE", "1").lower() in ("1", "true", "yes")
CANDIDATE_TTL_MINUTES = int(os.getenv("CANDIDATE_TTL_MINUTES", "60"))
DEEP_SCAN_TOP = int(os.getenv("DEEP_SCAN_TOP", "60"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "62"))
MIN_DIRECTION_GAP = int(os.getenv("MIN_DIRECTION_GAP", "8"))
MAX_24H_MOVE = float(os.getenv("MAX_24H_MOVE", "7.0"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "100000"))
WORKERS = int(os.getenv("WORKERS", "4"))
TIMEOUT = int(os.getenv("TIMEOUT", "18"))
DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).resolve().parent / "data")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE = str(DATA_DIR / "toobit_crypto_hunter_v3.db")
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "1800"))
KLINE_5M_LIMIT = int(os.getenv("KLINE_5M_LIMIT", "55"))
KLINE_1H_LIMIT = int(os.getenv("KLINE_1H_LIMIT", "45"))

# ntfy settings (used only by the ntfy version)
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "").strip()
NTFY_SERVER = (os.getenv("NTFY_SERVER") or "https://ntfy.sh").rstrip("/")

LOG_FILE = DATA_DIR / "hunter.log"
_file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
_stream_handler = logging.StreamHandler()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[_file_handler, _stream_handler],
    force=True,
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Toobit-Crypto-PreMove-Hunter/2.0"})


# ---------------- HTTP / PARSING ----------------
def api_get(path, params=None):
    last_error = None
    for attempt in range(3):
        try:
            response = SESSION.get(BASE + path, params=params, timeout=TIMEOUT)
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(1.5 * (attempt + 1))
                response.raise_for_status()
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                code = payload.get("code")
                if code not in (None, 200, "200"):
                    raise RuntimeError("API response code: " + str(code) + " | " + str(payload)[:300])
            return payload
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.8 * (attempt + 1))
    raise RuntimeError(f"GET {path} failed after retries: {last_error}")


def number(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pick(obj, keys, default=None):
    if not isinstance(obj, dict):
        return default
    for key in keys:
        if key in obj and obj[key] is not None:
            return obj[key]
    return default


def unwrap_list(payload, preferred=()):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in preferred:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in preferred:
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def init_db():
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS oi_snapshots(
                symbol TEXT NOT NULL, ts INTEGER NOT NULL, value REAL NOT NULL,
                PRIMARY KEY(symbol, ts)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts(
                symbol TEXT NOT NULL, side TEXT NOT NULL, ts INTEGER NOT NULL,
                PRIMARY KEY(symbol, side, ts)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candidates(
                symbol TEXT PRIMARY KEY, side TEXT NOT NULL, first_seen INTEGER NOT NULL,
                last_checked INTEGER NOT NULL, last_score INTEGER NOT NULL, status TEXT NOT NULL
            )
        """)
        conn.commit()


def record_oi_and_delta(symbol, current):
    if current is None or current <= 0:
        return None
    now = int(time.time())
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        old = conn.execute(
            "SELECT value FROM oi_snapshots WHERE symbol=? AND ts<=? ORDER BY ts DESC LIMIT 1",
            (symbol, now - 25 * 60)
        ).fetchone()
        conn.execute(
            "INSERT OR REPLACE INTO oi_snapshots(symbol,ts,value) VALUES(?,?,?)",
            (symbol, now, current)
        )
        conn.execute("DELETE FROM oi_snapshots WHERE ts<?", (now - 2 * 86400,))
        conn.commit()
    if old and old[0] and old[0] > 0:
        return (current / old[0] - 1.0) * 100.0
    return None


# ---------------- MARKET UNIVERSE ----------------
def get_crypto_usdt_contracts():
    payload = api_get("/api/v1/exchangeInfo")
    contracts = unwrap_list(payload, ("contracts",))
    eligible = []
    excluded_tradfi = 0

    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        symbol = pick(contract, ("symbol", "contractCode", "s"))
        quote = pick(contract, ("quoteAsset", "quoteCoin", "marginAsset"))
        status = pick(contract, ("status", "state"))
        inverse = bool(contract.get("inverse", False))
        categories = contract.get("categories") or []
        is_rwa = bool(contract.get("isRwa", False))

        # Strict USDT-margined, active, non-inverse contracts only.
        if not symbol or quote != "USDT" or status != "TRADING" or inverse:
            continue

        # Toobit labels stock/index-like contracts under TradFi categories.
        category_text = " ".join(str(x).lower() for x in categories)
        if "tradfi" in category_text or is_rwa:
            excluded_tradfi += 1
            continue

        eligible.append(symbol)

    eligible = sorted(set(eligible))
    if not eligible:
        raise RuntimeError(
            "No eligible crypto USDT-M contracts parsed. Check exchangeInfo schema/status."
        )
    return eligible, excluded_tradfi


def get_ticker_map():
    payload = api_get("/quote/v1/contract/ticker/24hr")
    rows = unwrap_list(payload, ("ticker", "tickers", "list"))
    result = {}
    for row in rows:
        if isinstance(row, dict):
            symbol = pick(row, ("s", "symbol"))
            if symbol:
                result[symbol] = row
    if not result:
        raise RuntimeError("Could not parse the all-contract 24h ticker response.")
    return result


def get_funding_map():
    # Toobit returns all funding rows when symbol is omitted.
    payload = api_get("/api/v1/futures/fundingRate")
    rows = unwrap_list(payload, ("list", "fundingRate"))
    result = {}
    for row in rows:
        if isinstance(row, dict):
            symbol = pick(row, ("symbol", "s"))
            rate = number(pick(row, ("rate", "fundingRate")))
            if symbol and rate is not None:
                result[symbol] = rate
    return result


# ---------------- PER-SYMBOL DATA ----------------
def parse_candle(row):
    # Official Toobit futures kline array:
    # [openTime, open, high, low, close, volume, ignored, quoteVolume,
    #  trades, takerBuyBaseVolume, takerBuyQuoteVolume]
    if isinstance(row, (list, tuple)) and len(row) >= 11:
        return {
            "open_time": number(row[0], 0),
            "open": number(row[1], 0),
            "high": number(row[2], 0),
            "low": number(row[3], 0),
            "close": number(row[4], 0),
            "quote_volume": number(row[7], 0),
            "taker_buy_quote": number(row[10], 0),
        }
    if isinstance(row, dict):
        return {
            "open_time": number(pick(row, ("t", "openTime")), 0),
            "open": number(pick(row, ("o", "open")), 0),
            "high": number(pick(row, ("h", "high")), 0),
            "low": number(pick(row, ("l", "low")), 0),
            "close": number(pick(row, ("c", "close")), 0),
            "quote_volume": number(pick(row, ("q", "quoteVolume", "quoteAssetVolume")), 0),
            "taker_buy_quote": number(pick(row, ("Q", "takerBuyQuoteVolume")), 0),
        }
    raise RuntimeError("Unexpected kline row format")


def get_candle_data(symbol, interval, limit):
    payload = api_get("/quote/v1/klines", {
        "symbol": symbol, "interval": interval, "limit": limit
    })
    rows = unwrap_list(payload, ("klines", "list"))
    parsed = [parse_candle(row) for row in rows]
    parsed = [row for row in parsed if row["close"] > 0 and row["high"] > 0 and row["low"] > 0]
    if len(parsed) < 25:
        raise RuntimeError(f"Not enough {interval} candles: {len(parsed)}")

    interval_seconds = {"1m": 60, "3m": 180, "5m": 300, "15m": 900,
                        "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
                        "1d": 86400}.get(interval)
    if not interval_seconds:
        raise RuntimeError(f"Unsupported interval for live-candle detection: {interval}")

    last = parsed[-1]
    open_time = last["open_time"] or 0
    # Toobit timestamps may be milliseconds or seconds.
    if open_time > 10_000_000_000:
        open_time /= 1000.0
    now = time.time()
    is_forming = open_time <= now < open_time + interval_seconds
    if is_forming:
        return parsed[:-1], last
    return parsed, None


def get_closed_candles(symbol, interval, limit):
    closed, _forming = get_candle_data(symbol, interval, limit)
    return closed


def get_open_interest(symbol):
    payload = api_get("/quote/v1/openInterest", {"symbol": symbol})
    rows = unwrap_list(payload, ("openInterestList",))
    if rows and isinstance(rows[0], dict):
        return number(pick(rows[0], ("size", "openInterest", "oi")))
    if isinstance(payload, dict):
        return number(pick(payload, ("size", "openInterest", "oi")))
    return None


def get_depth_imbalance(symbol):
    payload = api_get("/quote/v1/depth", {"symbol": symbol, "limit": 20})
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    bids = payload.get("b", []) if isinstance(payload, dict) else []
    asks = payload.get("a", []) if isinstance(payload, dict) else []
    bid_value = sum(number(level[0], 0) * number(level[1], 0)
                    for level in bids if isinstance(level, (list, tuple)) and len(level) >= 2)
    ask_value = sum(number(level[0], 0) * number(level[1], 0)
                    for level in asks if isinstance(level, (list, tuple)) and len(level) >= 2)
    total = bid_value + ask_value
    return (bid_value - ask_value) / total if total > 0 else 0.0


# ---------------- ANALYSIS ----------------
def atr(candles, period=14):
    values = []
    for index in range(1, len(candles)):
        high = candles[index]["high"]
        low = candles[index]["low"]
        previous_close = candles[index - 1]["close"]
        values.append(max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close)
        ))
    if len(values) < period:
        return None
    return statistics.mean(values[-period:])


def analyze_symbol(symbol, ticker, funding_rate):
    candles_5m, forming_5m = get_candle_data(symbol, "5m", KLINE_5M_LIMIT)
    candles_1h = get_closed_candles(symbol, "1h", KLINE_1H_LIMIT)

    price = number(pick(ticker, ("c", "lastPrice", "last", "close")))
    day_move = number(pick(ticker, ("pcp", "priceChangePercent", "changePercent")), 0.0)
    quote_volume_24h = number(pick(ticker, ("qv", "quoteVolume", "quoteVol")), 0.0)
    if not price or price <= 0:
        raise RuntimeError("Ticker has no valid last price")
    if abs(day_move) > MAX_24H_MOVE:
        return None
    if quote_volume_24h < MIN_24H_QUOTE_VOLUME:
        return None

    previous_20 = candles_5m[-20:]
    avg_quote_volume = statistics.mean(c["quote_volume"] for c in previous_20)
    live_progress = 0.0
    live_volume_ratio = None
    if forming_5m is not None:
        open_time = forming_5m["open_time"] or 0
        if open_time > 10_000_000_000:
            open_time /= 1000.0
        live_progress = max(0.0, min(1.0, (time.time() - open_time) / 300.0))
        # Project partial volume to a full 5m candle. Only score it after 20% elapsed.
        if avg_quote_volume > 0 and live_progress >= 0.20:
            live_volume_ratio = (forming_5m["quote_volume"] / avg_quote_volume) / live_progress
        taker_source = forming_5m
        candle_volume_ratio = (forming_5m["quote_volume"] / avg_quote_volume
                               if avg_quote_volume > 0 else 0.0)
    else:
        last_5m = candles_5m[-1]
        candle_volume_ratio = (last_5m["quote_volume"] / avg_quote_volume
                               if avg_quote_volume > 0 else 0.0)
        taker_source = last_5m
    if forming_5m is not None:
        # Do not score a very early partial candle before enough time has elapsed.
        volume_ratio = live_volume_ratio if live_volume_ratio is not None else 0.0
    else:
        volume_ratio = candle_volume_ratio
    taker_share = (
        taker_source["taker_buy_quote"] / taker_source["quote_volume"]
        if taker_source["quote_volume"] > 0 else 0.5
    )
    range_high = max(c["high"] for c in previous_20)
    range_low = min(c["low"] for c in previous_20)
    compression_high = max(c["high"] for c in candles_5m[-12:])
    compression_low = min(c["low"] for c in candles_5m[-12:])
    compression_pct = (
        (compression_high - compression_low) / price * 100 if price else 999
    )

    atr_1h = atr(candles_1h, 14)
    if not atr_1h or atr_1h <= 0:
        return None
    atr_1h_pct = atr_1h / price * 100

    # Multi-hour trend context: 20-candle 1h average and recent 3h direction.
    closes_1h = [c["close"] for c in candles_1h]
    sma_20 = statistics.mean(closes_1h[-20:])
    momentum_3h = (
        (closes_1h[-1] / closes_1h[-4] - 1) * 100
        if closes_1h[-4] else 0.0
    )

    oi_current = get_open_interest(symbol)
    oi_change = record_oi_and_delta(symbol, oi_current)
    imbalance = get_depth_imbalance(symbol)

    long_score = 0
    short_score = 0
    long_reasons = []
    short_reasons = []

    # Shared volatility / participation evidence.
    if volume_ratio >= 2.0:
        long_score += 18; short_score += 18
        volume_label = "حجم تخمینی کندل زنده حداقل ۲ برابر میانگین" if forming_5m is not None and live_volume_ratio is not None else "حجم کندل ۵دقیقه‌ای حداقل ۲ برابر میانگین"
        long_reasons.append(volume_label)
        short_reasons.append(volume_label)
    elif volume_ratio >= 1.5:
        long_score += 11; short_score += 11
        volume_label = "افزایش حجم تخمینی کندل زنده" if forming_5m is not None and live_volume_ratio is not None else "افزایش حجم کندل ۵دقیقه‌ای"
        long_reasons.append(volume_label)
        short_reasons.append(volume_label)

    if oi_change is not None:
        if oi_change >= 1.5:
            long_score += 12; short_score += 12
            long_reasons.append("رشد Open Interest")
            short_reasons.append("رشد Open Interest")
        elif oi_change <= -1.5:
            long_score -= 5; short_score -= 5

    if compression_pct <= 2.5:
        long_score += 6; short_score += 6
        long_reasons.append("فشردگی نوسان کوتاه‌مدت")
        short_reasons.append("فشردگی نوسان کوتاه‌مدت")

    # Live candle is observed before close; it is explicitly labelled as provisional.
    if forming_5m is not None:
        live_range_pct = ((forming_5m["high"] - forming_5m["low"]) / price * 100) if price else 0
        if live_range_pct >= 0.35:
            long_score += 3; short_score += 3
            long_reasons.append("دامنه کندل ۵دقیقه‌ای در حال تشکیل در حال گسترش است")
            short_reasons.append("دامنه کندل ۵دقیقه‌ای در حال تشکیل در حال گسترش است")

    # Direction-specific evidence. Price near BOTH edges cannot trigger both sides:
    # a final score gap is required below.
    long_distance = (range_high - price) / price * 100
    short_distance = (price - range_low) / price * 100

    if 0 <= long_distance <= 0.65:
        long_score += 13
        long_reasons.append("قیمت نزدیک سقف محدوده ۲۰ کندل")
    elif forming_5m is not None and price >= range_high and (price / range_high - 1) * 100 <= 0.35:
        long_score += 15
        long_reasons.append("شکست/عبور اولیه سقف محدوده در کندل زنده؛ هنوز تأیید نهایی نیست")
    if 0 <= short_distance <= 0.65:
        short_score += 13
        short_reasons.append("قیمت نزدیک کف محدوده ۲۰ کندل")
    elif forming_5m is not None and price <= range_low and (range_low / price - 1) * 100 <= 0.35:
        short_score += 15
        short_reasons.append("شکست/عبور اولیه کف محدوده در کندل زنده؛ هنوز تأیید نهایی نیست")

    if taker_share >= 0.58:
        long_score += 11
        long_reasons.append("غلبه خرید تهاجمی در حجم معاملات")
    elif taker_share <= 0.42:
        short_score += 11
        short_reasons.append("غلبه فروش تهاجمی در حجم معاملات")

    if imbalance >= 0.15:
        long_score += 10
        long_reasons.append("دفتر سفارش متمایل به خریداران")
    elif imbalance <= -0.15:
        short_score += 10
        short_reasons.append("دفتر سفارش متمایل به فروشندگان")

    if closes_1h[-1] > sma_20 and momentum_3h > 0:
        long_score += 12
        long_reasons.append("روند یک‌ساعته و مومنتوم صعودی")
    elif closes_1h[-1] < sma_20 and momentum_3h < 0:
        short_score += 12
        short_reasons.append("روند یک‌ساعته و مومنتوم نزولی")

    # Extreme funding is a crowding warning, not a standalone direction signal.
    if funding_rate is not None:
        if funding_rate >= 0.001:
            long_score -= 7
            long_reasons.append("هشدار ازدحام لانگ به‌علت Funding بالا")
        elif funding_rate <= -0.001:
            short_score -= 7
            short_reasons.append("هشدار ازدحام شورت به‌علت Funding منفی شدید")

    long_score = max(0, min(100, long_score))
    short_score = max(0, min(100, short_score))
    gap = abs(long_score - short_score)

    if max(long_score, short_score) < MIN_SCORE or gap < MIN_DIRECTION_GAP:
        return None

    side = "LONG" if long_score > short_score else "SHORT"
    score = max(long_score, short_score)

    # Scenario levels use 1h ATR, not tiny 5m ATR. They are volatility scenarios,
    # not guaranteed 5–15% targets and not trade instructions.
    multipliers = (2.5, 4.0, 6.0)
    if side == "LONG":
        targets = [price + atr_1h * m for m in multipliers]
        invalidation = min(range_low, price - 1.2 * atr_1h)
        reasons = long_reasons
    else:
        targets = [price - atr_1h * m for m in multipliers]
        invalidation = max(range_high, price + 1.2 * atr_1h)
        reasons = short_reasons

    target_pcts = [abs(target / price - 1) * 100 for target in targets]
    return {
        "symbol": symbol,
        "side": side,
        "score": score,
        "other_score": min(long_score, short_score),
        "price": price,
        "day_move": day_move,
        "volume_24h": quote_volume_24h,
        "volume_ratio": volume_ratio,
        "candle_state": "FORMING" if forming_5m is not None else "CLOSED_ONLY",
        "candle_progress_pct": live_progress * 100 if forming_5m is not None else 100.0,
        "oi_change": oi_change,
        "funding": funding_rate,
        "taker_share": taker_share,
        "imbalance": imbalance,
        "atr_1h_pct": atr_1h_pct,
        "targets": targets,
        "target_pcts": target_pcts,
        "invalidation": invalidation,
        "reasons": reasons,
    }


# ---------------- ALERTS ----------------
def format_alert(item):
    oi_text = "داده تاریخی کافی نیست" if item["oi_change"] is None else f'{item["oi_change"]:+.2f}%'
    return (
        f'\n{"=" * 48}\n'
        f'{item["symbol"]} | {item["side"]} | امتیاز غربالگری {item["score"]}/100\n'
        f'امتیاز جهت مخالف: {item["other_score"]}/100\n'
        f'قیمت: {item["price"]:.10g} | تغییر ۲۴ساعته: {item["day_move"]:+.2f}%\n'
        f'حجم ۲۴ساعته: {item["volume_24h"]:,.0f} USDT\n'
        f'حجم کندل ۵دقیقه: {item["volume_ratio"]:.2f}x | وضعیت: {item["candle_state"]} ({item["candle_progress_pct"]:.0f}%) | تغییر OI: {oi_text}\n'
        f'Taker buy share: {item["taker_share"]:.2f} | دفتر سفارش: {item["imbalance"]:+.2f}\n'
        f'Funding: {item["funding"] if item["funding"] is not None else "N/A"}\n'
        f'ATR یک‌ساعته: {item["atr_1h_pct"]:.2f}%\n'
        f'اهداف سناریویی مبتنی بر ATR یک‌ساعته:\n'
        f'  1) {item["targets"][0]:.10g} ({item["target_pcts"][0]:.2f}%)\n'
        f'  2) {item["targets"][1]:.10g} ({item["target_pcts"][1]:.2f}%)\n'
        f'  3) {item["targets"][2]:.10g} ({item["target_pcts"][2]:.2f}%)\n'
        f'ابطال سناریو: {item["invalidation"]:.10g}\n'
        f'دلایل: {"؛ ".join(item["reasons"])}\n'
        f'UTC: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}\n'
        'هشدار پژوهشی/مراقبتی است؛ پیش‌بینی قطعی یا توصیه معامله نیست. اجرای معامله فقط دستی.'
    )


def send_alert(title, item):
    message = format_alert(item)
    print(message, flush=True)
    # The no-ntfy version leaves this function as console-only.
    if ENABLE_NTFY:
        if not NTFY_TOPIC:
            print("NTFY_TOPIC تنظیم نشده؛ هشدار فقط در لاگ سرور ثبت شد.")
            return
        try:
            response = requests.post(
                f"{NTFY_SERVER.rstrip('/')}/{NTFY_TOPIC}",
                data=message.encode("utf-8"),
                headers={
                    "Title": title,
                    "Priority": "high" if item["score"] >= 78 else "default",
                },
                timeout=15,
            )
            response.raise_for_status()
            print("ntfy: اعلان ارسال شد.")
        except Exception as exc:
            logging.error("ntfy send failed: %s", exc)


# ---------------- SCAN LOOP ----------------
def is_recent_duplicate(symbol, side):
    cutoff = int(time.time()) - COOLDOWN_SECONDS
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        row = conn.execute(
            "SELECT 1 FROM alerts WHERE symbol=? AND side=? AND ts>? LIMIT 1",
            (symbol, side, cutoff)
        ).fetchone()
        return row is not None


def mark_alert_sent(symbol, side):
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute(
            "INSERT INTO alerts(symbol,side,ts) VALUES(?,?,?)",
            (symbol, side, int(time.time()))
        )
        conn.commit()


def get_active_candidates():
    cutoff = int(time.time()) - CANDIDATE_TTL_MINUTES * 60
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        rows = conn.execute(
            "SELECT symbol, side, first_seen, last_checked, last_score FROM candidates "
            "WHERE status='ACTIVE' AND last_checked>=?", (cutoff,)
        ).fetchall()
        conn.execute("UPDATE candidates SET status='EXPIRED' WHERE status='ACTIVE' AND last_checked<?", (cutoff,))
        conn.execute("DELETE FROM alerts WHERE ts<?", (int(time.time()) - 14 * 86400,))
        conn.commit()
    return {row[0]: {"side": row[1], "first_seen": row[2], "last_checked": row[3], "score": row[4]} for row in rows}


def save_candidate(item, existing=None):
    now = int(time.time())
    first_seen = existing["first_seen"] if existing else now
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute(
            "INSERT INTO candidates(symbol,side,first_seen,last_checked,last_score,status) "
            "VALUES(?,?,?,?,?,'ACTIVE') ON CONFLICT(symbol) DO UPDATE SET "
            "side=excluded.side,last_checked=excluded.last_checked,last_score=excluded.last_score,status='ACTIVE'",
            (item["symbol"], item["side"], first_seen, now, item["score"])
        )
        conn.commit()


def invalidate_candidate(symbol, previous, reason):
    now = int(time.time())
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("UPDATE candidates SET status='INVALID',last_checked=? WHERE symbol=?", (now, symbol))
        conn.commit()
    message = (
        f"{symbol} | شکار قبلی نامعتبر شد\n"
        f"جهت قبلی: {previous['side']} | امتیاز قبلی: {previous['score']}/100\n"
        f"علت: {reason}\n"
        f"زمان UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n"
        "این پیام فقط اعتبارسنجی پژوهشی است؛ معامله خودکار انجام نمی‌شود."
    )
    print(message, flush=True)
    if ENABLE_NTFY and NTFY_TOPIC:
        try:
            response = requests.post(
                f"{NTFY_SERVER}/{NTFY_TOPIC}", data=message.encode("utf-8"),
                headers={"Title": f"INVALIDATED: {symbol}", "Priority": "high"}, timeout=15
            )
            response.raise_for_status()
        except Exception as exc:
            logging.error("ntfy invalidation failed: %s", exc)


def run_scan():
    init_db()
    active = get_active_candidates()
    symbols, excluded_tradfi = get_crypto_usdt_contracts()
    tickers = get_ticker_map()
    funding = get_funding_map()

    ranked = []
    for symbol in symbols:
        ticker = tickers.get(symbol)
        if not ticker:
            continue
        qv = number(pick(ticker, ("qv", "quoteVolume", "quoteVol")), 0.0)
        move = number(pick(ticker, ("pcp", "priceChangePercent", "changePercent")), 0.0)
        price = number(pick(ticker, ("c", "lastPrice", "last", "close")))
        if price and qv >= MIN_24H_QUOTE_VOLUME and abs(move) <= MAX_24H_MOVE:
            ranked.append((qv, symbol, ticker))

    ranked.sort(reverse=True, key=lambda row: row[0])
    selected = ranked[:DEEP_SCAN_TOP]
    work = {symbol: ticker for _, symbol, ticker in selected}
    # Previously detected candidates are rechecked even if they fall out of the top-volume shortlist.
    for symbol in active:
        if symbol in tickers:
            work[symbol] = tickers[symbol]

    print(f"\nکریپتوهای فعال USDT-M: {len(symbols)} | قراردادهای TradFi حذف‌شده: {excluded_tradfi}")
    print(f"فیلتر اولیه: {len(ranked)} | تحلیل عمیق جدید: {len(selected)} | شکارهای قبلی برای اعتبارسنجی مجدد: {len(active)}")
    if not work:
        print("کاندیدایی برای تحلیل عمیق باقی نماند.")
        return

    results = {}
    errors = set()
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(analyze_symbol, symbol, ticker, funding.get(symbol)): symbol
            for symbol, ticker in work.items()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                results[symbol] = future.result()
            except Exception as exc:
                errors.add(symbol)
                logging.warning("%s | %s", symbol, exc)

    new_count = 0
    valid_count = 0
    invalid_count = 0
    for symbol, item in results.items():
        previous = active.get(symbol)
        if previous:
            if item is None:
                invalidate_candidate(symbol, previous, "امتیاز یا نشانه‌های لازم دیگر از حد اعتبارسنجی عبور نمی‌کنند")
                invalid_count += 1
                continue
            if item["side"] != previous["side"]:
                invalidate_candidate(symbol, previous, f"جهت غالب از {previous['side']} به {item['side']} تغییر کرده است")
                invalid_count += 1
                save_candidate(item)
                send_alert(f'NEW {item["side"]}: {symbol}', item)
                new_count += 1
            else:
                save_candidate(item, previous)
                valid_count += 1
        elif item is not None:
            save_candidate(item)
            send_alert(f'NEW {item["side"]}: {symbol}', item)
            new_count += 1

    print(
        f"\nپایان اسکن | شکار جدید: {new_count} | همچنان معتبر: {valid_count} | "
        f"نامعتبرشده: {invalid_count} | خطاهای داده/API: {len(errors)}"
    )
    if len(selected) < len(symbols):
        print("همه قراردادها در مرحله تیکر/فیلتر اولیه بررسی شدند؛ تحلیل عمیق فقط روی فهرست منتخب و شکارهای قبلی انجام شد.")
    if errors:
        print("خطای API به‌معنای شکار نامعتبر نیست؛ نمادهای خطادار در این دور بدون تغییر وضعیت باقی ماندند.")


def main():
    print("Toobit Crypto Pre-Move Hunter v3 | GitHub Actions | فقط داده عمومی | بدون معامله")
    print("اسکن زمان‌بندی‌شده: هر ۶ دقیقه | کندل ۵دقیقه‌ای زنده + بسته‌شده | اعتبارسنجی مجدد شکارها")
    if RUN_ONCE:
        run_scan()
        return
    while True:
        started = time.time()
        try:
            run_scan()
        except KeyboardInterrupt:
            print("اسکنر متوقف شد.")
            break
        except Exception as exc:
            logging.exception("خطای کلی اسکن: %s", exc)
        time.sleep(max(5, SCAN_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    main()
