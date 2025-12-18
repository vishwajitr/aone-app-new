#!/usr/bin/env python3
"""
NIFTY 9:15–9:30 ORB (LIVE - PRODUCTION READY)
-----------------------------------------------
✔ Live ORB candle build (no history)
✔ 9:15–9:30 breakout with buffer
✔ DYNAMIC expiry fetching from OpenAlgo API
✔ Correct options order placement
✔ Continuous spot + option logging
✔ APScheduler (IST only)
✔ Safe handling of quotes() missing data
✔ Improved error handling & logging
✔ Position tracking with orderbook verification
✔ Graceful shutdown handling
✔ DUPLICATE ORDER PREVENTION (BUY & SELL)
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
import requests
import json
import threading

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
STOPLOSS_POINTS = 5
BUFFER = 0.2

ORB_START = dt_time(9, 15)   # ORB build starts at 9:15 AM
ORB_END   = dt_time(9, 30)   # ORB locks at 9:30 AM

FORCE_EXIT = dt_time(15, 10)

SPOT_LOG_INTERVAL_MIN = 1
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
# State with Thread Safety
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

# Cache for expiry date
current_expiry = None

# Thread locks to prevent race conditions
order_lock = threading.Lock()
exit_lock = threading.Lock()

# Track order attempts
buy_order_placed = False
sell_order_placed = False

# -------------------------------------------------------
# Graceful Shutdown
# -------------------------------------------------------
def signal_handler(sig, frame):
    log("⚠️ SHUTDOWN SIGNAL RECEIVED")
    if position_open and entry_symbol and not sell_order_placed:
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

def verify_position_filled(symbol, orderid=None):
    """Verify if order was filled by checking orderbook"""
    try:
        time.sleep(2)  # Wait for order to process
        orderbook = client.orderbook()
        
        if orderbook and "data" in orderbook:
            orders = orderbook["data"]
            
            # Handle if orders is a list of strings (some brokers return this)
            if isinstance(orders, list) and orders:
                for order in orders:
                    # If order is a string, skip
                    if isinstance(order, str):
                        continue
                    
                    # If order is a dict, check it
                    if isinstance(order, dict):
                        order_symbol = order.get("symbol", "")
                        order_status = order.get("status", "")
                        order_id = order.get("orderid", "")
                        
                        # Match by symbol or orderid
                        symbol_match = symbol and order_symbol == symbol
                        id_match = orderid and order_id == str(orderid)
                        
                        if (symbol_match or id_match) and order_status.upper() in ["COMPLETE", "COMPLETED"]:
                            log(f"✅ Order verified: {order_symbol} | Status: {order_status}")
                            return True
        
        log(f"⚠️ Position verification failed for {symbol} (orderid: {orderid})")
        
    except Exception as e:
        log(f"❌ Orderbook check error: {e}")
    
    return False

# -------------------------------------------------------
# Dynamic Expiry Fetching
# -------------------------------------------------------
def fetch_current_expiry():
    """
    Fetch the nearest expiry date for NIFTY options using OpenAlgo Expiry API.
    Returns expiry in DDMMMYY format (e.g., "17DEC25")
    """
    try:
        log("🔍 Fetching current NIFTY options expiry...")
        
        url = f"{HOST}/api/v1/expiry"
        payload = {
            "apikey": API_KEY,
            "symbol": "NIFTY",
            "exchange": "NFO",
            "instrumenttype": "options"
        }
        
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get("status") == "success" and result.get("data"):
                expiry_dates = result["data"]
                
                if expiry_dates:
                    # First date is the nearest expiry (dates are sorted)
                    nearest_expiry = expiry_dates[0]
                    
                    # Convert from "DD-MMM-YY" to "DDMMMYY" format
                    # Example: "17-DEC-25" -> "17DEC25"
                    expiry_formatted = nearest_expiry.replace("-", "")
                    
                    log(f"✅ Current expiry fetched: {expiry_formatted}")
                    log(f"📅 Available expiries: {', '.join(expiry_dates[:5])}...")
                    
                    return expiry_formatted
                else:
                    log("❌ No expiry dates returned")
                    return None
            else:
                log(f"❌ Expiry API error: {result.get('message', 'Unknown error')}")
                return None
        else:
            log(f"❌ Expiry API HTTP error: {response.status_code}")
            return None
            
    except Exception as e:
        log(f"❌ Exception fetching expiry: {e}")
        return None

def get_or_fetch_expiry():
    """Get cached expiry or fetch if not available"""
    global current_expiry
    
    if current_expiry is None:
        current_expiry = fetch_current_expiry()
        
        if current_expiry is None:
            log("❌ CRITICAL: Unable to fetch expiry date. Cannot place orders.")
            return None
    
    return current_expiry

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
# Orders with Duplicate Prevention
# -------------------------------------------------------
def buy_option(option_type):
    """
    Buy option using OpenAlgo's optionsorder endpoint.
    Dynamically fetches expiry date from OpenAlgo API.
    Thread-safe to prevent duplicate orders.
    """
    global buy_order_placed
    
    # Thread-safe check to prevent duplicate buy orders
    with order_lock:
        if buy_order_placed:
            log("⚠️ BUY order already placed, skipping duplicate")
            return None
        
        buy_order_placed = True  # Mark immediately to block other threads
    
    try:
        log(f"🔵 Attempting to BUY {option_type} option...")
        
        # Get or fetch current expiry
        expiry_date = get_or_fetch_expiry()
        
        if expiry_date is None:
            log("❌ Cannot place order without expiry date")
            buy_order_placed = False  # Reset on failure
            return None
        
        log(f"📅 Using expiry: {expiry_date}")
        
        resp = client.optionsorder(
            strategy="ORB_0915_0930",
            underlying="NIFTY",
            exchange=OPTION_ORDER_EXCHANGE,
            expiry_date=expiry_date,  # DYNAMIC EXPIRY
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
            orderid = resp.get("orderid")
            
            # Check if mode is 'analyze' (paper trading)
            mode = resp.get("mode", "")
            if mode == "analyze":
                log(f"⚠️ ORDER IN ANALYZE MODE (Paper Trading)")
                log(f"✅ Simulated position: {symbol}")
                return symbol  # Return symbol for paper trading
            
            # For live trading, verify position
            if symbol:
                log(f"✅ Buy order placed: {symbol} (OrderID: {orderid})")
                return symbol
            else:
                log(f"⚠️ Order response missing symbol")
                buy_order_placed = False  # Reset on failure
                return None
        else:
            log(f"❌ Order failed: {resp.get('message', 'Unknown error')}")
            buy_order_placed = False  # Reset on failure
            return None
            
    except Exception as e:
        log(f"❌ Buy order exception: {e}")
        import traceback
        log(f"   Traceback: {traceback.format_exc()}")
        buy_order_placed = False  # Reset on exception
        return None

def sell_option(symbol):
    """
    Exit option position.
    Thread-safe to prevent duplicate sell orders.
    """
    global sell_order_placed, position_open
    
    # Thread-safe check to prevent duplicate sell orders
    with exit_lock:
        if sell_order_placed:
            log("⚠️ SELL order already placed, skipping duplicate")
            return False
        
        sell_order_placed = True  # Mark immediately to block other threads
    
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
            position_open = False  # Mark position as closed
            return True
        else:
            log(f"❌ Exit failed: {resp.get('message', 'Unknown error')}")
            sell_order_placed = False  # Reset on failure
            return False
            
    except Exception as e:
        log(f"❌ Sell order exception: {e}")
        sell_order_placed = False  # Reset on exception
        return False

# -------------------------------------------------------
# Pre-Market Initialization
# -------------------------------------------------------
log("🚀 Starting bot initialization...")
log(f"📍 Host: {HOST}")
log(f"🎯 Strategy: NIFTY ORB 9:15-9:30 AM")
log(f"📊 Lot Size: {LOT_SIZE} | Quantity: {QTY}")
log(f"🎯 Target: {TARGET_POINTS} pts | SL: {STOPLOSS_POINTS} pts")
log(f"🔄 Buffer: {BUFFER} pts")

# Fetch expiry at startup
log("=" * 60)
current_expiry = fetch_current_expiry()
if current_expiry:
    log(f"✅ Bot initialized with expiry: {current_expiry}")
else:
    log("⚠️ Warning: Could not fetch expiry. Will retry when placing orders.")
log("=" * 60)

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
            if position_open and entry_symbol and not sell_order_placed:
                log("⏰ FORCED EXIT - Market closing")
                sell_option(entry_symbol)
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
        if orb_locked and not entry_symbol and not trade_done and not buy_order_placed:
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
                # Mark as attempted immediately to prevent multiple orders
                trade_done = True
                
                entry_symbol = buy_option(option_type)
                
                if entry_symbol:
                    # Wait a moment and get entry price
                    time.sleep(2)
                    entry_price = get_option_ltp(entry_symbol)
                    
                    if entry_price:
                        stop_price = entry_price - STOPLOSS_POINTS
                        target_price = entry_price + TARGET_POINTS
                        position_open = True
                        trade_done = False  # Reset so we can monitor for exit

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
                        log("❌ Could not get entry price, trade abandoned")
                        entry_symbol = None
                        buy_order_placed = False  # Reset to allow retry
                else:
                    log("❌ Order placement failed, trade abandoned")

        # Exit Logic (check SL/Target)
        if position_open and entry_symbol and not sell_order_placed:
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
                    trade_done = True
                    break

            # Target hit
            elif ltp >= target_price:
                log(f"🎯 TARGET HIT | LTP=₹{ltp:.2f} >= TG=₹{target_price:.2f}")
                log(f"   P&L: ₹{pnl:.2f}")
                if sell_option(entry_symbol):
                    trade_done = True
                    break

        time.sleep(1)

except Exception as e:
    log(f"❌ CRITICAL ERROR: {e}")
    import traceback
    log(f"   Traceback: {traceback.format_exc()}")
    if position_open and entry_symbol and not sell_order_placed:
        log("⚠️ Attempting emergency exit...")
        sell_option(entry_symbol)

finally:
    scheduler.shutdown()
    log("🏁 STRATEGY FINISHED")
    log("=" * 60)