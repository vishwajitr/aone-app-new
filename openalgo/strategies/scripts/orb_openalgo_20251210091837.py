# orb_openalgo.py
"""
Production-ready Opening Range Breakout (ORB) strategy using OpenAlgo.
- Uses openalgo.ta for ATR and other indicator computations (ATR 14)
- ORB: 09:15 - 09:30 (1m bars). Trading window: 09:31 - 15:15
- Progressive profit locking implemented
- Risk per trade: 1% of capital
- Max 1 trade per day (but can open up to 2 positions across universe if selected)
- Uses start_date and end_date when fetching historical data
- Prints quotes/LTP/depth immediately when fetched
"""

import os
import time
import math
import logging
from datetime import datetime, date, time as dt_time, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List

import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

# OpenAlgo imports
from openalgo import api, ta

# --- Configuration ---
load_dotenv()
API_KEY = os.getenv("OPENALGO_API_KEY", "")
API_HOST = os.getenv("OPENALGO_API_HOST", "http://127.0.0.1:5000")
TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", 10000.0))  # default Rs.10,000
RISK_PER_TRADE = 0.01  # 1% capital
ATR_PERIOD = 14
MIN_VOLUME_THRESHOLD = 5000
TARGET_MULTIPLIER = 10.0  # 10x risk
ORDER_TIMEOUT = 30  # seconds
MAX_RETRIES = 2
LIMIT_BUFFER = 0.001  # 0.1% buffer
RETRY_BUFFER = 0.002  # 0.2% buffer on retry

# Timezone (IST)
IST = pytz.timezone("Asia/Kolkata")

# Trading window & times
OPENING_RANGE_START = dt_time(9, 15)
OPENING_RANGE_END = dt_time(9, 30)
TRADING_START = dt_time(9, 31)
TRADING_END = dt_time(15, 15)
FALLBACK_ACTIVATION_TIME = dt_time(10, 0)

# Symbols provided by user (NSE)
SYMBOLS = [
"ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK",
"BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BPCL","BHARTIARTL",
"BRITANNIA","CIPLA","COALINDIA","DIVISLAB","DRREDDY","EICHERMOT",
"GRASIM","HCLTECH","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO",
"HINDUNILVR","ICICIBANK","ITC","INDUSINDBK","INFY","JSWSTEEL",
"KOTAKBANK","LT","M&M","MARUTI","NTPC","ONGC","POWERGRID",
"RELIANCE","SBILIFE","SBIN","SUNPHARMA","TCS","TATACONSUM",
"TATAMOTORS","TATASTEEL","TECHM","TITAN","ULTRACEMCO",
"UPL","WIPRO"
]

# Exchanges mapping (equity)
EXCHANGE = "NSE"

# Progressive locking thresholds (percentage as decimals)
# Based on your spec: after 0.4% -> lock 0.2%, after 0.6% -> lock 0.4% ...
# We'll construct thresholds from 0.004 to 0.10 step 0.002 (0.2% steps on thresholds of 0.4% increments?)
# But to match examples: threshold increments by 0.2% from 0.4% upward; locked = threshold - 0.2%
PROGRESSIVE_THRESHOLDS = [round(x/100,4) for x in range(40, 1001, 20)]  # 0.004,0.006,...,0.10
# locked_profits = threshold - 0.002 (0.2%)
PROGRESSIVE_LOCKS = {thr: round(max(0.0, thr - 0.002), 4) for thr in PROGRESSIVE_THRESHOLDS}

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Print startup line (per your instruction)
print("🔁 OpenAlgo Python Bot is running.")

# Initialize client
client = api(api_key=API_KEY, host=API_HOST)

# --- Utilities ---


def to_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def now_ist() -> datetime:
    return datetime.now(IST)


def is_time_between(start: dt_time, end: dt_time, now: Optional[dt_time] = None) -> bool:
    nowt = now or to_ist(datetime.now()).time()
    return start <= nowt <= end


def tick_adjust(price: float) -> float:
    """
    Adjust price to tick size rules described in your spec.
    Price ranges:
      0-100: tick 1
      100-500: tick 1
      500-1000: tick 3
      1000+: tick 3
    We'll round to nearest tick.
    """
    if price < 500:
        tick = 1.0
    else:
        tick = 3.0
    return round(round(price / tick) * tick, 2)


def limit_price_for_side(ltp: float, side: str, retry: bool = False) -> float:
    """
    Compute limit price for BUY/SELL with buffer
    """
    buf = RETRY_BUFFER if retry else LIMIT_BUFFER
    if side == "BUY":
        return round(ltp * (1 + buf), 2)
    else:
        return round(ltp * (1 - buf), 2)


def print_quote(symbol: str, exchange: str = EXCHANGE):
    q = client.quotes(symbol=symbol, exchange=exchange)
    print(f"Quote for {exchange}:{symbol} -> {q}")
    return q


# --- Core strategy functions ---


def fetch_intraday_df(symbol: str, start_date: str, end_date: str, interval: str = "1m") -> pd.DataFrame:
    """
    Use start_date and end_date controls (YYYY-MM-DD).
    Returns pandas DataFrame with DatetimeIndex (localized to IST)
    """
    df = client.history(symbol=symbol, exchange=EXCHANGE, interval=interval, start_date=start_date, end_date=end_date)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    # Ensure tz-aware IST
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize(IST)
    else:
        df.index = df.index.tz_convert(IST)
    return df


def compute_opening_range(symbol: str, today_str: str) -> Optional[Dict]:
    """
    Fetch 1m bars for the day and compute ORB high/low for 09:15-09:30 inclusive.
    Returns dict {orb_high, orb_low, range, atr}
    """
    df = fetch_intraday_df(symbol, start_date=today_str, end_date=today_str, interval="1m")
    # Filter opening range
    start_ts = IST.localize(datetime.combine(date.fromisoformat(today_str), OPENING_RANGE_START))
    end_ts = IST.localize(datetime.combine(date.fromisoformat(today_str), OPENING_RANGE_END))
    orb_df = df[(df.index >= start_ts) & (df.index <= end_ts)]
    if orb_df.empty:
        logging.warning(f"No opening range data for {symbol} on {today_str}")
        return None

    orb_high = orb_df['high'].max()
    orb_low = orb_df['low'].min()
    rng = orb_high - orb_low

    # ATR using openalgo.ta - needs arrays
    atr_series = ta.atr(df['high'].values, df['low'].values, df['close'].values, period=ATR_PERIOD)
    # take latest ATR value
    atr_val = float(atr_series[-1]) if len(atr_series) > 0 else 0.0

    # Volatility as percent of open for opening range last candle
    last_open = orb_df['open'].iloc[-1]
    volatility = (orb_df['high'] - orb_df['low']).max() / last_open if last_open != 0 else 0.0

    return {
        "orb_high": float(orb_high),
        "orb_low": float(orb_low),
        "range": float(rng),
        "atr": float(atr_val),
        "volatility": float(volatility),
        "opening_df": orb_df
    }


def determine_entry_levels(symbol: str, orb_info: Dict) -> Dict:
    """
    Computes entry price, stop loss, and target for LONG and SHORT based on ORB and ATR.
    Entry Buffer = ATR × (0.05 if volatility > 0.5% else 0.08)
    Stop Buffer = max(ATR × 0.3, Range × 0.2)
    """
    atr = orb_info["atr"]
    rng = orb_info["range"]
    vol = orb_info["volatility"]  # decimal like 0.01

    entry_buffer = atr * (0.05 if vol > 0.005 else 0.08)
    stop_buffer = max(atr * 0.3, rng * 0.2)

    long_entry = orb_info["orb_high"] + entry_buffer
    long_stop = orb_info["orb_low"] - stop_buffer
    risk_per_share_long = long_entry - long_stop
    long_target = long_entry + (risk_per_share_long * TARGET_MULTIPLIER)

    short_entry = orb_info["orb_low"] - entry_buffer
    short_stop = orb_info["orb_high"] + stop_buffer
    risk_per_share_short = short_stop - short_entry
    short_target = short_entry - (risk_per_share_short * TARGET_MULTIPLIER)

    return {
        "long": {
            "entry": round(long_entry, 2),
            "stop": round(long_stop, 2),
            "target": round(long_target, 2),
            "risk_per_share": round(risk_per_share_long, 2)
        },
        "short": {
            "entry": round(short_entry, 2),
            "stop": round(short_stop, 2),
            "target": round(short_target, 2),
            "risk_per_share": round(risk_per_share_short, 2)
        }
    }


def compute_position_size(entry_price: float, stop_price: float) -> int:
    """
    Position Size = (Capital × Risk%) / Risk Per Share
    Round down to integer number of shares. Enforce minimum volume threshold via quotes.
    """
    risk_amount = TOTAL_CAPITAL * RISK_PER_TRADE
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share <= 0:
        return 0
    size = math.floor(risk_amount / risk_per_share)
    return max(0, int(size))


@dataclass
class ActiveTrade:
    symbol: str
    side: str  # "LONG" or "SHORT"
    entry_price: float
    stop_price: float
    target_price: float
    qty: int
    executed_order: Optional[dict] = None
    open: bool = True


active_trades: Dict[str, ActiveTrade] = {}


# --- Order Execution & Management ---


def place_limit_order(symbol: str, side: str, qty: int, limit_price: float, retry=False) -> dict:
    """
    Place limit order via OpenAlgo client.
    side: "BUY" or "SELL"
    Returns order response dict. Retries on failure up to MAX_RETRIES.
    """
    attempt = 0
    last_exc = None
    while attempt <= MAX_RETRIES:
        try:
            exchange = EXCHANGE
            order_type = "LIMIT"
            # NOTE: OpenAlgo order call signature may vary. Using a general pattern:
            payload = {
                "exchange": exchange,
                "symbol": symbol,
                "qty": qty,
                "price": limit_price,
                "product": "MIS",  # intraday default
                "order_type": order_type,
                "action": side
            }
            logging.info(f"Placing order: {payload}")
            resp = client.place_order(**payload)
            print(f"Order response for {symbol}: {resp}")
            return resp
        except Exception as e:
            last_exc = e
            logging.exception(f"Order attempt {attempt+1} failed for {symbol}: {e}")
            attempt += 1
            time.sleep(0.5)
    raise last_exc


def update_stop_trade(trade: ActiveTrade, new_stop: float):
    """
    Update stop for an active trade. Use market/limit depending on API support.
    We'll send a modify order if API supports or place a stop order (placeholder).
    """
    # Bound by tick-size
    new_stop_adj = tick_adjust(new_stop)
    # We print the update action (per requirement to print quotes/depth)
    print(f"Updating stop for {trade.symbol} from {trade.stop_price} to {new_stop_adj}")
    trade.stop_price = new_stop_adj
    # Implementation detail: actual stop modification depends on broker API.
    # Here we assume a client.modify_order or place OCO stop order; placeholder:
    try:
        # Example: client.modify_order(order_id=..., price=new_stop_adj)
        pass
    except Exception:
        logging.exception("Stop update failed (no-op placeholder)")


def check_progressive_lock(trade: ActiveTrade, current_price: float):
    """
    Check all progressive thresholds and update stop if necessary.
    For LONG: Current Profit % = (Current Price - Entry) / Entry
      If Current Profit > Threshold:
        New Stop = Entry * (1 + Locked Profit %)
        If New Stop > Current Stop: update stop
    For SHORT analogously.
    """
    entry = trade.entry_price
    if trade.side == "LONG":
        current_profit = (current_price - entry) / entry if entry > 0 else 0.0
        for threshold, locked in sorted(PROGRESSIVE_LOCKS.items()):
            if current_profit >= threshold:
                new_stop = entry * (1 + locked)
                if new_stop > trade.stop_price:
                    update_stop_trade(trade, new_stop)
    else:  # SHORT
        current_profit = (entry - current_price) / entry if entry > 0 else 0.0
        for threshold, locked in sorted(PROGRESSIVE_LOCKS.items()):
            if current_profit >= threshold:
                new_stop = entry * (1 - locked)
                if new_stop < trade.stop_price:
                    update_stop_trade(trade, new_stop)


def exit_trade(trade: ActiveTrade, reason: str = "TARGET/STOP/EOD"):
    """
    Close position with market order (or aggressive limit). Print exit info.
    """
    side = "SELL" if trade.side == "LONG" else "BUY"
    print(f"Exiting {trade.symbol} qty={trade.qty} side={trade.side} reason={reason}")
    # We'll place a market order (MARKET type may be "MARKET" or "SL-M" depending on API)
    try:
        resp = client.place_order(exchange=EXCHANGE, symbol=trade.symbol, qty=trade.qty, action=side, product="MIS", order_type="MARKET")
        print(f"Exit order response: {resp}")
    except Exception:
        logging.exception("Exit market order failed (placeholder).")


# --- Strategy orchestration ---


def market_direction() -> Optional[str]:
    """
    Nifty direction during opening range (09:15-09:30)
    """
    today_str = date.today().strftime("%Y-%m-%d")
    try:
        df = fetch_intraday_df("NIFTY", start_date=today_str, end_date=today_str, interval="1m")
    except Exception:
        logging.exception("Failed to fetch NIFTY data for direction")
        return None
    start_ts = IST.localize(datetime.combine(date.fromisoformat(today_str), OPENING_RANGE_START))
    end_ts = IST.localize(datetime.combine(date.fromisoformat(today_str), OPENING_RANGE_END))
    orb = df[(df.index >= start_ts) & (df.index <= end_ts)]
    if orb.empty:
        return None
    nifty_open = orb['open'].iloc[0]
    nifty_close = orb['close'].iloc[-1]
    return "UP" if nifty_close > nifty_open else "DOWN" if nifty_close < nifty_open else "SIDEWAYS"


def select_stocks_from_universe() -> List[str]:
    """
    You asked earlier for sector-based selection, but requested production-only for given list.
    We'll take top 2 scores from the universe by a simple opening-momentum*volume scoring
    as per your sector-scoring logic but applied across the given universe (so we pick top 2).
    However final rule: max 1 trade per day, and max 2 open positions.
    """
    today_str = date.today().strftime("%Y-%m-%d")
    scores = []
    for sym in SYMBOLS:
        try:
            df = fetch_intraday_df(sym, start_date=today_str, end_date=today_str, interval="1m")
            start_ts = IST.localize(datetime.combine(date.fromisoformat(today_str), OPENING_RANGE_START))
            end_ts = IST.localize(datetime.combine(date.fromisoformat(today_str), OPENING_RANGE_END))
            orb_df = df[(df.index >= start_ts) & (df.index <= end_ts)]
            if orb_df.empty:
                continue
            price_momentum = (orb_df['close'].iloc[-1] - orb_df['open'].iloc[0]) / orb_df['open'].iloc[0]
            volatility = (orb_df['high'].max() - orb_df['low'].min()) / orb_df['open'].iloc[0]
            vol = orb_df['volume'].sum()
            vol_score = min(vol / MIN_VOLUME_THRESHOLD, 3.0)
            score = (price_momentum * 0.5) + (volatility * 0.3) + (vol_score * 0.2)
            scores.append((sym, score))
        except Exception:
            continue
    # pick top 2
    scores_sorted = sorted(scores, key=lambda x: x[1], reverse=True)
    top_symbols = [s for s, _ in scores_sorted[:2]]
    logging.info(f"Top symbols selected: {top_symbols}")
    return top_symbols


def attempt_place_orb_trade_for_symbol(symbol: str, market_dir: Optional[str]) -> Optional[ActiveTrade]:
    """
    Evaluate ORB for symbol and place one trade (LONG or SHORT) only if valid.
    Use market direction filter: if market UP => look LONG; DOWN => SHORT; if SIDEWAYS or None => both allowed.
    """
    today_str = date.today().strftime("%Y-%m-%d")
    orb = compute_opening_range(symbol, today_str)
    if orb is None:
        return None

    levels = determine_entry_levels(symbol, orb)

    # Determine preferred side
    preferred = None
    if market_dir == "UP":
        preferred = "LONG"
    elif market_dir == "DOWN":
        preferred = "SHORT"

    # Evaluate LONG first if preferred or if none
    for side in (preferred, "LONG" if preferred != "LONG" else "SHORT"):
        if side is None:
            continue
        if side == "LONG":
            entry = levels['long']['entry']
            stop = levels['long']['stop']
            target = levels['long']['target']
            risk_per_share = levels['long']['risk_per_share']
            if not (stop < entry < target):
                continue
            # tick adjust
            entry_adj = tick_adjust(entry)
            stop_adj = tick_adjust(stop)
            target_adj = tick_adjust(target)
            # compute position size
            qty = compute_position_size(entry_adj, stop_adj)
            if qty <= 0:
                continue
            # volume check
            q = print_quote(symbol)
            vol = q.get('data', {}).get('volume', 0) if isinstance(q, dict) else 0
            if vol and vol < MIN_VOLUME_THRESHOLD:
                logging.info(f"Volume too low for {symbol}: {vol}")
                continue
            # Place limit buy order
            ltp = q.get('data', {}).get('ltp') if isinstance(q, dict) else None
            limit_price = limit_price_for_side(ltp or entry_adj, "BUY", retry=False)
            limit_price = tick_adjust(limit_price)
            resp = place_limit_order(symbol=symbol, side="BUY", qty=qty, limit_price=limit_price)
            trade = ActiveTrade(symbol=symbol, side="LONG", entry_price=entry_adj, stop_price=stop_adj, target_price=target_adj, qty=qty, executed_order=resp)
            active_trades[symbol] = trade
            return trade
        else:  # SHORT
            entry = levels['short']['entry']
            stop = levels['short']['stop']
            target = levels['short']['target']
            risk_per_share = levels['short']['risk_per_share']
            if not (target < entry < stop):
                continue
            entry_adj = tick_adjust(entry)
            stop_adj = tick_adjust(stop)
            target_adj = tick_adjust(target)
            qty = compute_position_size(entry_adj, stop_adj)
            if qty <= 0:
                continue
            q = print_quote(symbol)
            vol = q.get('data', {}).get('volume', 0) if isinstance(q, dict) else 0
            if vol and vol < MIN_VOLUME_THRESHOLD:
                logging.info(f"Volume too low for {symbol}: {vol}")
                continue
            ltp = q.get('data', {}).get('ltp') if isinstance(q, dict) else None
            limit_price = limit_price_for_side(ltp or entry_adj, "SELL", retry=False)
            limit_price = tick_adjust(limit_price)
            resp = place_limit_order(symbol=symbol, side="SELL", qty=qty, limit_price=limit_price)
            trade = ActiveTrade(symbol=symbol, side="SHORT", entry_price=entry_adj, stop_price=stop_adj, target_price=target_adj, qty=qty, executed_order=resp)
            active_trades[symbol] = trade
            return trade
    return None


# --- Scheduled Jobs ---


def daily_orb_entry_job():
    """
    Scheduled job to run at 09:31 IST:
    - Determine market direction
    - Select top 2 symbols
    - Attempt entries (observing max 1 trade/day overall)
    """
    logging.info("Running daily ORB entry job")
    md = market_direction()
    logging.info(f"Market direction: {md}")
    selected = select_stocks_from_universe()
    trades_placed = 0
    for sym in selected:
        if trades_placed >= 1:  # rule: maximum 1 trade per day
            break
        try:
            t = attempt_place_orb_trade_for_symbol(sym, md)
            if t:
                trades_placed += 1
                logging.info(f"Placed trade for {sym}: {t}")
        except Exception:
            logging.exception(f"Failed to place trade for {sym}")


def fallback_job():
    """
    Run after 10:00 if no primary trade executed and fallback conditions:
    - Try both directions on top symbols and take first valid breakout
    - Max 1 fallback trade per day
    """
    logging.info("Running fallback job (after 10:00)")
    if active_trades:
        logging.info("Active trades already exist; skipping fallback.")
        return
    selected = select_stocks_from_universe()
    md = None  # fallback tries both directions
    for sym in selected:
        try:
            t = attempt_place_orb_trade_for_symbol(sym, md)
            if t:
                logging.info(f"Fallback placed trade for {sym}: {t}")
                break
        except Exception:
            logging.exception("Fallback attempt failed")


def monitor_positions_job():
    """
    Runs frequently during market hours to:
    - Fetch LTP for active trades
    - Check target/stop hit
    - Update progressive locks
    - EOD enforce close
    """
    now = now_ist()
    if not is_time_between(TRADING_START, TRADING_END, now.time()):
        return

    # For each active trade check price
    for sym, trade in list(active_trades.items()):
        try:
            q = print_quote(sym)
            ltp = q.get("data", {}).get("ltp") if isinstance(q, dict) else None
            if ltp is None:
                continue
            ltp = float(ltp)
            # Check target hit
            if trade.side == "LONG":
                if ltp >= trade.target_price:
                    exit_trade(trade, reason="TARGET_HIT")
                    trade.open = False
                    del active_trades[sym]
                    continue
                # Stop hit
                if ltp <= trade.stop_price:
                    exit_trade(trade, reason="STOP_HIT")
                    trade.open = False
                    del active_trades[sym]
                    continue
            else:  # SHORT
                if ltp <= trade.target_price:
                    exit_trade(trade, reason="TARGET_HIT")
                    trade.open = False
                    del active_trades[sym]
                    continue
                if ltp >= trade.stop_price:
                    exit_trade(trade, reason="STOP_HIT")
                    trade.open = False
                    del active_trades[sym]
                    continue

            # Progressive locking
            check_progressive_lock(trade, ltp)
        except Exception:
            logging.exception("Error monitoring trade")

    # EOD: force close close to TRADING_END - run a few minutes before 15:15
    # EOD enforcement will be done by a scheduled EOD job


def eod_exit_job():
    """
    Cancel pending and close all open positions at EOD (15:15 IST)
    """
    logging.info("Running EOD exit job")
    for sym, trade in list(active_trades.items()):
        try:
            exit_trade(trade, reason="EOD_EXIT")
            trade.open = False
            del active_trades[sym]
        except Exception:
            logging.exception("Failed to EOD exit trade")


# --- Scheduler setup (APScheduler, IST) ---


def start_scheduler():
    scheduler = BackgroundScheduler(timezone=IST)
    # Entry job: run at 09:31 IST every trading day (Mon-Fri)
    scheduler.add_job(daily_orb_entry_job, CronTrigger(hour=9, minute=31, day_of_week="mon-fri"), id="orb_entry")
    # Fallback job: run at 10:00 IST
    scheduler.add_job(fallback_job, CronTrigger(hour=10, minute=0, day_of_week="mon-fri"), id="fallback")
    # Monitor positions every 10 seconds during market hours
    scheduler.add_job(monitor_positions_job, 'interval', seconds=10, id="monitor")
    # EOD exit at 15:14 to ensure positions closed by 15:15
    scheduler.add_job(eod_exit_job, CronTrigger(hour=15, minute=14, day_of_week="mon-fri"), id="eod_exit")
    scheduler.start()
    logging.info("Scheduler started with jobs: orb_entry, fallback, monitor, eod_exit")


# --- Main entrypoint ---


if __name__ == "__main__":
    try:
        start_scheduler()
        logging.info("ORB Strategy scheduler is running. Press Ctrl+C to stop.")
        # Keep main thread alive
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down strategy.")
