#!/usr/bin/env python3
"""
NIFTY 15-min First Candle ATM Breakout Strategy
------------------------------------------------
- First 15-min candle (9:15–9:30) -> HIGH/LOW
- Breakout above HIGH -> Buy ATM CE
- Breakdown below LOW -> Buy ATM PE
- Target = +30 option points
- Stoploss = -15 option points
- One trade per day (strict)
- Exact contract exit using resolved option symbol
- Forced exit at 15:10 IST
- Uses OpenAlgo optionsorder + placeorder
"""

import os
import time
import pytz
import threading
from datetime import datetime, time as dt_time
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
QTY = LOT_SIZE

SPOT = "NIFTY"
SPOT_EX = "NSE_INDEX"
OPT_EX = "NFO"

TARGET_POINTS = 30
STOPLOSS_POINTS = 15

ENTRY_HOUR = 9
ENTRY_MIN = 31
EXIT_TIME = "15:10"

IST = pytz.timezone("Asia/Kolkata")
scheduler = BackgroundScheduler(timezone=IST)
stop_flag = threading.Event()

# -------------------------------------------------------
# State
# -------------------------------------------------------
state = {
    "first_high": None,
    "first_low": None,
    "expiry": None,
    "entry_side": None,
    "entry_symbol": None,
    "entry_price": None,
    "stop_price": None,
    "target_price": None,
    "qty": 0,
    "active": False,
    "trade_done": False,
}

# -------------------------------------------------------
# Utility
# -------------------------------------------------------
def now():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

# -------------------------------------------------------
# Spot + First Candle
# -------------------------------------------------------
def get_spot():
    try:
        q = client.quotes(symbol=SPOT, exchange=SPOT_EX)
        print(q)
        return q.get("data", {}).get("ltp")
    except Exception:
        traceback.print_exc()
    return None


def get_first_candle():
    today = datetime.now(IST).strftime("%Y-%m-%d")
    try:
        df = client.history(
            symbol=SPOT,
            exchange=SPOT_EX,
            interval="15m",
            start_date=today,
            end_date=today
        )
        print(df)
        for ts, r in df.iterrows():
            if ts.time() == dt_time(9, 15):
                return float(r.high), float(r.low)
        r = df.iloc[0]
        return float(r.high), float(r.low)
    except Exception:
        traceback.print_exc()
        return None, None

# -------------------------------------------------------
# Expiry
# -------------------------------------------------------
def get_expiry():
    try:
        r = client.expiry(symbol=SPOT, exchange=OPT_EX, instrumenttype="options")
        exp = r["data"][0]
        dt = datetime.strptime(exp, "%d-%b-%y")
        months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
        return f"{dt.day:02d}{months[dt.month-1]}{str(dt.year)[-2:]}"
    except Exception:
        traceback.print_exc()
    return None

# -------------------------------------------------------
# Orders
# -------------------------------------------------------
def open_option(option_type):
    try:
        resp = client.optionsorder(
            strategy="15MIN_BREAKOUT",
            underlying=SPOT,
            exchange=SPOT_EX,
            expiry_date=state["expiry"],
            offset="ATM",
            option_type=option_type,
            action="BUY",
            quantity=state["qty"],
            pricetype="MARKET",
            product="NRML"
        )
        print(resp)
        return resp.get("symbol")
    except Exception:
        traceback.print_exc()
        return None


def close_option():
    try:
        resp = client.placeorder(
            strategy="15MIN_BREAKOUT_EXIT",
            symbol=state["entry_symbol"],
            exchange=OPT_EX,
            action="SELL",
            price_type="MARKET",
            product="NRML",
            quantity=state["qty"]
        )
        print(resp)
    except Exception:
        traceback.print_exc()

# -------------------------------------------------------
# Prepare
# -------------------------------------------------------
def prepare():
    if state["trade_done"]:
        return

    h, l = get_first_candle()
    if not h:
        return

    state["first_high"] = h
    state["first_low"] = l
    state["expiry"] = get_expiry()
    state["qty"] = QTY
    state["active"] = True

    print(now(), f"Prepared | HIGH={h} LOW={l}")
    threading.Thread(target=monitor, daemon=True).start()

# -------------------------------------------------------
# Monitor
# -------------------------------------------------------
def monitor():
    while not stop_flag.is_set():
        try:
            spot = get_spot()
            if not spot:
                time.sleep(1)
                continue

            if state["active"] and not state["entry_side"]:
                if spot > state["first_high"]:
                    state["entry_side"] = "CE"
                    state["entry_symbol"] = open_option("CE")
                elif spot < state["first_low"]:
                    state["entry_side"] = "PE"
                    state["entry_symbol"] = open_option("PE")

                if state["entry_symbol"]:
                    ltp = client.quotes(symbol=state["entry_symbol"], exchange=OPT_EX)["data"]["ltp"]
                    state["entry_price"] = ltp
                    state["stop_price"] = ltp - STOPLOSS_POINTS
                    state["target_price"] = ltp + TARGET_POINTS
                    print(now(), f"ENTRY {state['entry_symbol']} @ {ltp}")

            if state["entry_symbol"]:
                ltp = client.quotes(symbol=state["entry_symbol"], exchange=OPT_EX)["data"]["ltp"]
                if ltp <= state["stop_price"]:
                    print(now(), "STOPLOSS HIT")
                    close_option()
                    break
                if ltp >= state["target_price"]:
                    print(now(), "TARGET HIT")
                    close_option()
                    break

            time.sleep(1)
        except Exception:
            traceback.print_exc()
            time.sleep(1)

    state["trade_done"] = True

# -------------------------------------------------------
# Forced Exit
# -------------------------------------------------------
def exit_job():
    if state.get("entry_symbol"):
        close_option()
    stop_flag.set()

# -------------------------------------------------------
# Scheduler
# -------------------------------------------------------
def schedule():
    scheduler.add_job(prepare, CronTrigger(hour=ENTRY_HOUR, minute=ENTRY_MIN))
    hh, mm = map(int, EXIT_TIME.split(":"))
    scheduler.add_job(exit_job, CronTrigger(hour=hh, minute=mm))

# -------------------------------------------------------
# Main
# -------------------------------------------------------
if __name__ == "__main__":
    schedule()
    scheduler.start()
    print(now(), "Waiting for trade...")
    while True:
        time.sleep(1)
