#!/usr/bin/env python3
"""
NIFTY 9:15–9:30 ORB (LIVE - PRODUCTION READY)
-----------------------------------------------
✔ Live ORB candle build (no history)
✔ 9:15–9:30 breakout with buffer
✔ NO manual expiry (OpenAlgo auto-resolves)
✔ Correct options order placement
✔ Continuous spot + option logging
✔ APScheduler (IST only)
✔ Safe handling of quotes() missing data
✔ Improved error handling & logging
✔ Position tracking with orderbook verification
✔ Graceful shutdown handling
"""

print("🔁 OpenAlgo Python Bot is running.")

# -------------------------------------------------------
# Imports
# -------------------------------------------------------
from openalgo import api
import time
import os
import signal
import sys
from datetime import datetime, time as dt_time
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
API_KEY = os.getenv("OPENALGO_APIKEY")
HOST = os.getenv("OPENALGO_API_HOST", "http://127.0.0.1:5000")

if not API_KEY:
    print("❌ ERROR: OPENALGO_APIKEY not set in environment")
    sys.exit(1)

SPOT_SYMBOL = "NIFTY"
SPOT_EXCHANGE = "NSE_INDEX"

OPTION_ORDER_EXCHANGE = "NSE"   # Correct exchange for optionsorder
OPTION_QUOTES_EXCHANGE = "NFO"  # Correct exchange for quotes

LOT_SIZE = 75
QTY = LOT_SIZE

TARGET_POINTS = 30
STOPLOSS_POINTS = 15
BUFFER = 0.2

ORB_START = dt_time(9, 15)   # ORB build starts at 9:15 AM
ORB_END   = dt_time(9, 30)   # ORB locks at 9:30 AM

FORCE_EXIT = dt_time(15, 10)

SPOT_LOG_INTERVAL_MIN = 5
OPTION_LOG_INTERVAL_MIN = 1
LOG_DIR = "logs"

# -------------------------------------------------------
# Setup
# -------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")
os.makedirs(LOG_DIR, exist_ok=True)

client = api(api_key=API_KEY, host=HOST)

scheduler = BackgroundScheduler(timezone=IST)
scheduler.start()

# -------------------------------------------------------
# State
# -------------------------------------------------------
first_high = None
first_low = None
orb_locked = False

entry_symbol = None
entry_price = None
stop_price = None
target_price = None
trade_done = False
position_open = False

# -------------------------------------------------------
# Graceful Shutdown
# -------------------------------------------------------
def signal_handler(sig, frame):
    log("⚠️ SHUTDOWN SIGNAL RECEIVED")
    if position_open and entry_symbol:
        log("⚠️ Attempting to close open position...")
        try:
            sell_option(entry_symbol)
        except Exception as e:
            log(f"❌ Error closing position: {e}")
    scheduler.shutdown()
    log("👋 Bot stopped")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# -------------------------------------------------------
# Utils
# -------------------------------------------------------
def log(msg):
    ts = datetime.now(IST).strftime('%H:%M:%S')
    print(f"{ts} | {msg}")
    # Also log to file
    log_to_file("bot_activity.log", msg)

def log_to_file(filename, msg):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(os.path.join(LOG_DIR, filename), "a") as f:
            f.write(f"{ts},{msg}\n")
    except Exception as e:
        print(f"❌ Log write error: {e}")

def get_spot():
    try:
        r = client.quotes(symbol=SPOT_SYMBOL, exchange=SPOT_EXCHANGE)
        if r and "data" in r and "ltp" in r["data"]:
            ltp = float(r["data"]["ltp"])
            if ltp > 0:  # Sanity check
                return ltp
    except Exception as e:
        log(f"❌ Spot fetch error: {e}")
    return None

def get_option_ltp(symbol):
    """Get LTP for option with error handling"""
    try:
        r = client.quotes(symbol=symbol, exchange=OPTION_QUOTES_EXCHANGE)
        if r and "data" in r and "ltp" in r["data"]:
            ltp = float(r["data"]["ltp"])
            if ltp > 0:  # Sanity check
                return ltp
    except Exception as e:
        log(f"❌ Option quote error for {symbol}: {e}")
    return None

def verify_position_filled(symbol):
    """Verify if order was filled by checking orderbook"""
    try:
        time.sleep(2)  # Wait for order to process
        orderbook = client.orderbook()
        if orderbook and "data" in orderbook:
            for order in orderbook["data"]:
                if (order.get("symbol") == symbol and 
                    order.get("status") in ["complete", "COMPLETE"]):
                    return True
        log(f"⚠️ Position verification failed for {symbol}")
    except Exception as e:
        log(f"❌ Orderbook check error: {e}")
    return False

# -------------------------------------------------------
# Logging Jobs
# -------------------------------------------------------
def spot_logger_job():
    spot = get_spot()
    if spot is None:
        return
    log_to_file("spot_log.csv", f"NIFTY,{spot}")
    print(f"📌 SPOT LOG | NIFTY={spot:.2f}")

def option_logger_job():
    global position_open
    if not entry_symbol or not position_open:
        return
    
    ltp = get_option_ltp(entry_symbol)
    if ltp is None:
        return
    
    pnl = (ltp - entry_price) * QTY
    log_to_file(
        "option_log.csv",
        f"{entry_symbol},{ltp},{entry_price},{stop_price},{target_price},{pnl:.2f}"
    )
    print(f"🎯 OPT LOG | {entry_symbol} LTP={ltp:.2f} PNL=₹{pnl:.2f}")

# Start spot logger immediately
scheduler.add_job(
    spot_logger_job,
    "interval",
    minutes=SPOT_LOG_INTERVAL_MIN,
    id="spot_logger",
    replace_existing=True
)

# -------------------------------------------------------
# Orders
# -------------------------------------------------------
def buy_option(option_type):
    """
    Buy option using OpenAlgo's optionsorder endpoint.
    expiry_date is NOT passed - OpenAlgo auto-resolves.
    """
    try:
        log(f"🔵 Attempting to BUY {option_type} option...")
        
        resp = client.optionsorder(
            strategy="ORB_0915_0930",
            underlying="NIFTY",
            exchange=OPTION_ORDER_EXCHANGE,
            offset="ATM",
            option_type=option_type,
            action="BUY",
            quantity=QTY,
            pricetype="MARKET",
            product="NRML"
        )

        log(f"📥 ORDER RESPONSE: {resp}")

        if resp.get("status") == "success":
            symbol = resp.get("symbol")
            if symbol and verify_position_filled(symbol):
                log(f"✅ Position confirmed: {symbol}")
                return symbol
            else:
                log(f"⚠️ Order placed but position not confirmed")
                return None
        else:
            log(f"❌ Order failed: {resp.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        log(f"❌ Buy order exception: {e}")
        return None

def sell_option(symbol):
    """Exit option position"""
    try:
        log(f"🔴 Attempting to SELL {symbol}...")
        
        resp = client.placeorder(
            strategy="ORB_0915_0930_EXIT",
            symbol=symbol,
            exchange=OPTION_QUOTES_EXCHANGE,
            action="SELL",
            price_type="MARKET",
            product="NRML",
            quantity=QTY
        )
        
        log(f"📥 EXIT RESPONSE: {resp}")
        
        if resp.get("status") == "success":
            log(f"✅ Exit order successful")
            return True
        else:
            log(f"❌ Exit failed: {resp.get('message', 'Unknown error')}")
            return False
            
    except Exception as e:
        log(f"❌ Sell order exception: {e}")
        return False

# -------------------------------------------------------
# Main Loop
# -------------------------------------------------------
log("⏳ Waiting for market to open (9:15 AM)...")

try:
    while True:
        now = datetime.now(IST).time()

        # Wait until ORB start time
        if now < ORB_START:
            time.sleep(1)
            continue

        # Force exit at 3:10 PM
        if now >= FORCE_EXIT:
            if position_open and entry_symbol:
                log("⏰ FORCED EXIT - Market closing")
                if sell_option(entry_symbol):
                    position_open = False
            break

        # Get current spot price
        spot = get_spot()
        if spot is None:
            time.sleep(1)
            continue

        # Build ORB candle (9:15 - 9:30)
        if ORB_START <= now <= ORB_END:
            if first_high is None:
                first_high = spot
                first_low = spot
                log(f"🕐 ORB BUILD STARTED | Spot={spot:.2f}")
            else:
                first_high = max(first_high, spot)
                first_low = min(first_low, spot)
                # Log ORB updates every 30 seconds
                if int(time.time()) % 30 == 0:
                    log(f"📊 ORB Building | H={first_high:.2f} L={first_low:.2f} Current={spot:.2f}")

        # Lock ORB at 9:30 AM
        if now > ORB_END and not orb_locked and first_high:
            orb_locked = True
            range_points = first_high - first_low
            log(f"🔒 ORB LOCKED | HIGH={first_high:.2f} LOW={first_low:.2f} RANGE={range_points:.2f}")

        # Entry Logic (after ORB locked)
        if orb_locked and not entry_symbol and not trade_done:
            signal_triggered = False
            option_type = None
            
            # Breakout above high
            if spot >= first_high + BUFFER:
                signal_triggered = True
                option_type = "CE"
                log(f"📈 BREAKOUT HIGH | Spot={spot:.2f} > ORB_HIGH+Buffer={first_high + BUFFER:.2f}")
            
            # Breakdown below low
            elif spot <= first_low - BUFFER:
                signal_triggered = True
                option_type = "PE"
                log(f"📉 BREAKOUT LOW | Spot={spot:.2f} < ORB_LOW-Buffer={first_low - BUFFER:.2f}")

            if signal_triggered:
                entry_symbol = buy_option(option_type)
                
                if entry_symbol:
                    # Wait a moment and get entry price
                    time.sleep(1)
                    entry_price = get_option_ltp(entry_symbol)
                    
                    if entry_price:
                        stop_price = entry_price - STOPLOSS_POINTS
                        target_price = entry_price + TARGET_POINTS
                        position_open = True

                        log(f"✅ ENTRY CONFIRMED")
                        log(f"   Symbol: {entry_symbol}")
                        log(f"   Entry: ₹{entry_price:.2f}")
                        log(f"   Stop: ₹{stop_price:.2f}")
                        log(f"   Target: ₹{target_price:.2f}")
                        log(f"   Risk: ₹{STOPLOSS_POINTS * QTY:.2f}")
                        log(f"   Reward: ₹{TARGET_POINTS * QTY:.2f}")

                        # Start option logger
                        scheduler.add_job(
                            option_logger_job,
                            "interval",
                            minutes=OPTION_LOG_INTERVAL_MIN,
                            id="option_logger",
                            replace_existing=True
                        )
                    else:
                        log("❌ Could not get entry price, skipping trade")
                        entry_symbol = None
                        trade_done = True

        # Exit Logic (check SL/Target)
        if position_open and entry_symbol:
            ltp = get_option_ltp(entry_symbol)
            
            if ltp is None:
                time.sleep(1)
                continue

            pnl = (ltp - entry_price) * QTY

            # Stoploss hit
            if ltp <= stop_price:
                log(f"🛑 STOPLOSS HIT | LTP=₹{ltp:.2f} <= SL=₹{stop_price:.2f}")
                log(f"   P&L: ₹{pnl:.2f}")
                if sell_option(entry_symbol):
                    position_open = False
                    trade_done = True
                    break

            # Target hit
            elif ltp >= target_price:
                log(f"🎯 TARGET HIT | LTP=₹{ltp:.2f} >= TG=₹{target_price:.2f}")
                log(f"   P&L: ₹{pnl:.2f}")
                if sell_option(entry_symbol):
                    position_open = False
                    trade_done = True
                    break

        time.sleep(1)

except Exception as e:
    log(f"❌ CRITICAL ERROR: {e}")
    if position_open and entry_symbol:
        log("⚠️ Attempting emergency exit...")
        sell_option(entry_symbol)

finally:
    scheduler.shutdown()
    log("🏁 STRATEGY FINISHED")
    log("=" * 60)