#!/usr/bin/env python3
"""
NIFTY 5-min First Candle ATM Breakout Strategy – FIXED
--------------------------------------------------------------------
Fix: ensure exits use the exact resolved option symbol opened earlier.
 - First 5-min candle (9:15–9:20) -> HIGH/LOW
 - Entry monitoring starts at configured time
 - Breakout above HIGH -> Buy ATM CE
 - Breakdown below LOW -> Buy ATM PE
 - SL = 30% | Target = 50%
 - One trade per day (strict)
 - No new leg until first is fully closed
 - Forced exit at 15:10 IST
 - Uses optionsorder() to open (auto-resolve), stores resolved symbol
 - Uses placeorder(symbol=resolved_symbol, ...) to close (exact symbol)
 - Prints all quotes immediately
"""

import os
import time
import pytz
import threading
from datetime import datetime, timedelta, time as dt_time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import traceback

from openalgo import api

# -------------------------------------------------------
# Startup Banner
# -------------------------------------------------------
print("🔁 OpenAlgo Python Bot is running.")

# -------------------------------------------------------
# Client Setup
# -------------------------------------------------------
API_KEY = os.getenv("OPENALGO_APIKEY", "52d589a0ae86e68f22ef820cd20272c33e579eb62cf30db18bc297b7c8b11e3c")
API_HOST = os.getenv("OPENALGO_API_HOST", "http://127.0.0.1:5000")

client = api(api_key=API_KEY, host=API_HOST)

# -------------------------------------------------------
# Strategy Settings
# -------------------------------------------------------
LOT_SIZE = 75
QTY = LOT_SIZE * 1

SPOT = "NIFTY"
SPOT_EX = "NSE_INDEX"
OPT_EX = "NFO"

SL_PCT = 0.30
TARGET_PCT = 0.50

# ENTRY time - set as you like
ENTRY_HOUR = 9
ENTRY_MIN = 53

EXIT_TIME = "15:10"

IST = pytz.timezone("Asia/Kolkata")
scheduler = BackgroundScheduler(timezone=IST)
stop_flag = threading.Event()

# -------------------------------------------------------
# State (One trade per day)
# -------------------------------------------------------
state = {
    "first_high": None,
    "first_low": None,
    "atm": None,
    "expiry": None,
    "entry_side": None,        # "CE" or "PE" or None
    "entry_symbol": None,      # <-- resolved symbol returned by optionsorder
    "entry_price": None,
    "stop_price": None,
    "target_price": None,
    "qty": 0,
    "active": False,
    "trade_done": False,      # 🚫 Prevents re-entry
}

# -------------------------------------------------------
# Utility
# -------------------------------------------------------
def now():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def print_quote(q):
    print("QUOTE:", q)
    try:
        if isinstance(q, dict):
            if "data" in q and isinstance(q["data"], dict):
                print("   >> LTP:", q["data"].get("ltp"))
            elif "ltp" in q:
                print("   >> LTP:", q.get("ltp"))
    except Exception:
        pass

def round_strike(v, step=50):
    return int(round(v / step) * step)

# -------------------------------------------------------
# Spot + Candle
# -------------------------------------------------------
def get_spot():
    try:
        q = client.quotes(symbol=SPOT, exchange=SPOT_EX)
        print_quote(q)
        if isinstance(q, dict) and "data" in q:
            return q["data"].get("ltp")
        if isinstance(q, dict) and "ltp" in q:
            return q.get("ltp")
    except Exception:
        traceback.print_exc()
    return None

def get_first_candle():
    today = datetime.now(IST).date()
    d = today.strftime("%Y-%m-%d")

    try:
        df = client.history(
            symbol=SPOT,
            exchange=SPOT_EX,
            interval="5m",
            start_date=d,
            end_date=d
        )
        print("History:", df)

        # ensure tz-aware index
        try:
            df.index = df.index.tz_localize(IST) if df.index.tz is None else df.index.tz_convert(IST)
        except Exception:
            pass

        for ts, r in df.iterrows():
            if ts.time() == dt_time(9, 15):
                return float(r.high), float(r.low)

        r = df.iloc[0]
        return float(r.high), float(r.low)
    except Exception:
        traceback.print_exc()
        return None, None

# -------------------------------------------------------
# Expiry + Option LTP by symbol
# -------------------------------------------------------
def get_expiry():
    try:
        r = client.expiry(symbol=SPOT, exchange=OPT_EX, instrumenttype="options")
        if isinstance(r, dict) and r.get("status") == "success":
            exp = r["data"][0]  # e.g. "11-DEC-25"
            dt = datetime.strptime(exp, "%d-%b-%y")
            months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
            return f"{dt.day:02d}{months[dt.month-1]}{str(dt.year)[-2:]}"
    except Exception:
        traceback.print_exc()
    return None

def get_ltp_by_symbol(symbol):
    if not symbol:
        return None
    try:
        q = client.quotes(symbol=symbol, exchange=OPT_EX)
        print_quote(q)
        if isinstance(q, dict) and "data" in q:
            return q["data"].get("ltp")
        if isinstance(q, dict) and "ltp" in q:
            return q.get("ltp")
    except Exception:
        traceback.print_exc()
    return None

# -------------------------------------------------------
# Open order (optionsorder) — returns resolved_symbol
# -------------------------------------------------------
def open_option_by_optionsorder(action, option_type, qty):
    """
    Uses optionsorder to open position and returns (resp, resolved_symbol).
    resp may contain order info; resolved_symbol is the full contract name if API provides it.
    """
    try:
        # Ensure expiry is set
        if not state.get("expiry"):
            state["expiry"] = get_expiry()
            if not state["expiry"]:
                print(now(), "❌ expiry not available")
                return None, None

        resp = client.optionsorder(
            strategy="BREAKOUT",
            underlying=SPOT,
            exchange=SPOT_EX,
            expiry_date=state["expiry"],
            offset="ATM",                # open ATM at time of order
            option_type=option_type,
            action=action,
            quantity=qty,
            pricetype="MARKET",
            product="NRML"
        )
        print("optionsorder response:", resp)
        resolved_symbol = None
        if isinstance(resp, dict):
            # response may include 'symbol' or 'contract' or similar
            resolved_symbol = resp.get("symbol") or resp.get("contract") or resp.get("option_symbol")
        return resp, resolved_symbol
    except Exception:
        traceback.print_exc()
        return None, None

# -------------------------------------------------------
# Close by exact resolved symbol (IMPORTANT FIX)
# -------------------------------------------------------
def close_option_by_symbol(action, resolved_symbol, qty):
    """
    Close using explicit symbol — do NOT re-resolve ATM.
    Use client.placeorder with 'symbol' argument to target exact contract.
    """
    if not resolved_symbol:
        print(now(), "❌ no resolved_symbol provided for close")
        return None

    try:
        resp = client.placeorder(
            strategy="BREAKOUT_CLOSE",
            symbol=resolved_symbol,     # exact contract name
            exchange=OPT_EX,
            action=action,              # SELL to exit if we opened BUY
            price_type="MARKET",
            product="NRML",
            quantity=str(qty)
        )
        print("placeorder (close) response:", resp)
        return resp
    except Exception:
        # Fallback: try optionsorder with offset="EXACT" + strike (if available)
        traceback.print_exc()
        try:
            # attempt to parse strike & type from resolved_symbol and use optionsorder with offset=EXACT
            # This is a best-effort fallback; primary path is placeorder with symbol.
            # Example resolved_symbol: NIFTY11DEC2525800CE -> parse strike and opt
            s = resolved_symbol
            # find last letters CE/PE
            if s.endswith("CE") or s.endswith("PE"):
                opt_type = s[-2:]
                # find digits before opt_type (strike)
                import re
                m = re.search(r"(\d+)(CE|PE)$", s)
                if m:
                    strike = m.group(1)
                    resp2 = client.optionsorder(
                        strategy="BREAKOUT",
                        underlying=SPOT,
                        exchange=SPOT_EX,
                        expiry_date=state.get("expiry"),
                        offset="EXACT",
                        option_type=opt_type,
                        action=action,
                        quantity=qty,
                        pricetype="MARKET",
                        product="NRML",
                        strike=strike
                    )
                    print("fallback optionsorder(close) resp:", resp2)
                    return resp2
        except Exception:
            traceback.print_exc()
        return None

# -------------------------------------------------------
# Reset (Permanently disables re-entry)
# -------------------------------------------------------
def reset_day():
    print(now(), "🧹 Resetting and locking further trades today")
    stop_flag.set()

    state["entry_side"] = None
    state["entry_symbol"] = None
    state["entry_price"] = None
    state["stop_price"] = None
    state["target_price"] = None
    state["qty"] = 0
    state["active"] = False
    state["trade_done"] = True     # LOCK — no further trades today

# -------------------------------------------------------
# Prepare
# -------------------------------------------------------
def prepare():
    print(now(), "🔍 Preparing strategy")

    if state["trade_done"]:
        print(now(), "Trade already done today — skipping prepare.")
        return

    high, low = get_first_candle()
    if high is None:
        print(now(), "❌ First candle not available — abort prepare")
        return

    state["first_high"] = high
    state["first_low"] = low

    spot = get_spot()
    if spot is None:
        print(now(), "❌ Spot not available")
        return

    atm = round_strike(spot)
    state["atm"] = atm
    state["qty"] = QTY

    exp = get_expiry()
    if exp is None:
        print(now(), "❌ expiry not available")
        return
    state["expiry"] = exp

    state["active"] = True
    print(now(), f"Prepared: HIGH={high} LOW={low} ATM={atm} EXP={exp}")

    start_monitor()

# -------------------------------------------------------
# Monitor
# -------------------------------------------------------
def start_monitor():
    stop_flag.clear()
    threading.Thread(target=monitor, daemon=True).start()

def monitor():
    poll = 1.2

    while not stop_flag.is_set():
        try:
            spot = get_spot()
            if spot is None:
                time.sleep(poll)
                continue

            # ENTRY: only if not already entered and trade not done
            if state["active"] and state["entry_side"] is None and not state["trade_done"]:
                # CE breakout
                if spot > state["first_high"]:
                    print(now(), "Signal: breakout above first_high — BUY CE")
                    resp, resolved_sym = open_option_by_optionsorder("BUY", "CE", state["qty"])
                    # store resolved symbol — critical fix:
                    state["entry_symbol"] = resolved_sym
                    # attempt to get LTP (either via order status or quote)
                    entry_price = None
                    if isinstance(resp, dict):
                        # try order id -> avg price, else fallback to quote
                        order_id = resp.get("orderid") or resp.get("order_id") or resp.get("id")
                        if order_id:
                            # try to read order status avg price (best-effort)
                            try:
                                status = client.orderstatus(order_id=order_id) if hasattr(client, "orderstatus") else None
                                if status:
                                    for key in ("avg_price", "avg_executed_price", "avgexecprice", "filled_price"):
                                        if key in status and status[key]:
                                            entry_price = float(status[key])
                                            break
                            except Exception:
                                pass
                    # fallback to quote of resolved symbol
                    if not entry_price and resolved_sym:
                        entry_price = get_ltp_by_symbol(resolved_sym)
                    state["entry_price"] = float(entry_price) if entry_price else None
                    if state["entry_price"]:
                        state["stop_price"] = state["entry_price"] * (1 - SL_PCT)
                        state["target_price"] = state["entry_price"] * (1 + TARGET_PCT)
                    state["entry_side"] = "CE"
                    print(now(), f"CE entry recorded: symbol={resolved_sym} price={state['entry_price']} stop={state['stop_price']} target={state['target_price']}")
                    # Do not allow any other entries (trade_done stays False until we close; but entry_side is set)
                    continue

                # PE breakdown
                if spot < state["first_low"]:
                    print(now(), "Signal: breakdown below first_low — BUY PE")
                    resp, resolved_sym = open_option_by_optionsorder("BUY", "PE", state["qty"])
                    state["entry_symbol"] = resolved_sym
                    entry_price = None
                    if isinstance(resp, dict):
                        order_id = resp.get("orderid") or resp.get("order_id") or resp.get("id")
                        if order_id:
                            try:
                                status = client.orderstatus(order_id=order_id) if hasattr(client, "orderstatus") else None
                                if status:
                                    for key in ("avg_price", "avg_executed_price", "avgexecprice", "filled_price"):
                                        if key in status and status[key]:
                                            entry_price = float(status[key])
                                            break
                            except Exception:
                                pass
                    if not entry_price and resolved_sym:
                        entry_price = get_ltp_by_symbol(resolved_sym)
                    state["entry_price"] = float(entry_price) if entry_price else None
                    if state["entry_price"]:
                        state["stop_price"] = state["entry_price"] * (1 - SL_PCT)
                        state["target_price"] = state["entry_price"] * (1 + TARGET_PCT)
                    state["entry_side"] = "PE"
                    print(now(), f"PE entry recorded: symbol={resolved_sym} price={state['entry_price']} stop={state['stop_price']} target={state['target_price']}")
                    continue

            # EXIT: use the resolved symbol stored on entry so we close exact contract
            if state["entry_side"] and state["entry_symbol"]:
                current_ltp = get_ltp_by_symbol(state["entry_symbol"])
                if current_ltp is None:
                    time.sleep(poll)
                    continue

                # Stoploss
                if state.get("stop_price") and current_ltp <= state["stop_price"]:
                    print(now(), "🔻 STOPLOSS HIT -> closing exact contract", state["entry_symbol"])
                    close_option_by_symbol("SELL", state["entry_symbol"], state["qty"])
                    reset_day()
                    break

                # Target
                if state.get("target_price") and current_ltp >= state["target_price"]:
                    print(now(), "🎯 TARGET HIT -> closing exact contract", state["entry_symbol"])
                    close_option_by_symbol("SELL", state["entry_symbol"], state["qty"])
                    reset_day()
                    break

            time.sleep(poll)
        except Exception:
            traceback.print_exc()
            time.sleep(1)

# -------------------------------------------------------
# Exit job at forced time
# -------------------------------------------------------
def exit_job():
    print(now(), "⏳ Forced exit time reached")
    if state.get("entry_symbol"):
        print(now(), "📤 Closing exact contract at exit:", state["entry_symbol"])
        close_option_by_symbol("SELL", state["entry_symbol"], state.get("qty", QTY))
    reset_day()

# -------------------------------------------------------
# Scheduler
# -------------------------------------------------------
def schedule():
    scheduler.add_job(
        prepare,
        CronTrigger(hour=ENTRY_HOUR, minute=ENTRY_MIN, timezone=IST),
        id="prepare"
    )
    hh, mm = map(int, EXIT_TIME.split(":"))
    scheduler.add_job(
        exit_job,
        CronTrigger(hour=hh, minute=mm, timezone=IST),
        id="exit"
    )
    print(now(), f"⏱ Scheduled: {ENTRY_HOUR:02d}:{ENTRY_MIN:02d} prepare | {EXIT_TIME} exit")

# -------------------------------------------------------
# Main
# -------------------------------------------------------
if __name__ == "__main__":
    schedule()
    scheduler.start()
    print(now(), "🚀 Waiting for prepare time...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(now(), "Interrupted by user — exiting.")
        try:
            reset_day()
        except:
            pass
        finally:
            scheduler.shutdown(wait=False)
