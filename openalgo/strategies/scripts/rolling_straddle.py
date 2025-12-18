import time as systime
from datetime import datetime, time as dtime
from apscheduler.schedulers.background import BackgroundScheduler
from openalgo import api
import pytz
import requests
import os
import signal
import sys

print("🔁 OpenAlgo Python Bot with Hedges is running.")

# === USER PARAMETERS ===

STRADDLE_ENTRY_HOUR = 9       # 10 for 10:40 AM
STRADDLE_ENTRY_MINUTE = 35    # 40 for 10:40 AM

SQUAREOFF_HOUR = 15           # 15 for 3:20 PM
SQUAREOFF_MINUTE = 20         # 20 for 3:20 PM

MAX_STRADDLES_PER_DAY = 3     # Daily limit on rolling straddles
ROLLING_THRESHOLD_PCT = 0.4   # Threshold for rolling (in percent, e.g. 0.4 means 0.4%)

HEDGE_DISTANCE = 500          # Hedge distance from ATM (500 points)

SYMBOL = "NIFTY"
EXCHANGE = "NSE_INDEX"
OPTION_EXCHANGE = "NFO"
OPTION_ORDER_EXCHANGE = "NSE"  # For optionsorder endpoint
STRIKE_INTERVAL = 50

STRATEGY = "ROLLING_STRADDLE_HEDGED"
LOT_SIZE = 75

API_KEY = os.getenv("OPENALGO_APIKEY", "52d589a0ae86e68f22ef820cd20272c33e579eb62cf30db18bc297b7c8b11e3c")
API_HOST = os.getenv("OPENALGO_API_HOST", "http://127.0.0.1:5000")

if not API_KEY:
    print("❌ ERROR: OPENALGO_APIKEY not set")
    sys.exit(1)

client = api(api_key=API_KEY, host=API_HOST)
IST = pytz.timezone("Asia/Kolkata")

# --- State ---
last_reference_spot = None
current_straddle_symbols = []  # Only straddle CE and PE
current_hedge_symbols = []     # Only hedge CE and PE
straddle_entry_count = 0
current_expiry = None  # Cache for expiry date
position_open = False

# -------------------------------------------------------
# Graceful Shutdown
# -------------------------------------------------------
def signal_handler(sig, frame):
    log("⚠️ SHUTDOWN SIGNAL RECEIVED")
    if position_open:
        log("⚠️ Attempting to close all positions...")
        try:
            close_all_positions()
        except Exception as e:
            log(f"❌ Error closing positions: {e}")
    log("👋 Bot stopped")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# -------------------------------------------------------
# Logging
# -------------------------------------------------------
def log(msg):
    ts = datetime.now(IST).strftime('%H:%M:%S')
    print(f"{ts} | {msg}")

# -------------------------------------------------------
# Dynamic Expiry Fetching (from ORB strategy)
# -------------------------------------------------------
def fetch_current_expiry():
    """
    Fetch the nearest expiry date for NIFTY options using OpenAlgo Expiry API.
    Returns expiry in DDMMMYY format (e.g., "17DEC25")
    """
    try:
        log("🔍 Fetching current NIFTY options expiry...")
        
        url = f"{API_HOST}/api/v1/expiry"
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
# Helper Functions
# -------------------------------------------------------
def get_atm_strike(spot):
    return int(round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL)

def get_spot():
    try:
        quote = client.quotes(symbol=SYMBOL, exchange=EXCHANGE)
        data = quote.get('data')
        if isinstance(data, list) and data:
            data = data[0]
        if data and 'ltp' in data:
            ltp = float(data['ltp'])
            if ltp > 0:
                return ltp
    except Exception as e:
        log(f"❌ Spot fetch error: {e}")
    return None

def verify_position_filled(symbol, orderid=None):
    """Verify if order was filled by checking orderbook"""
    try:
        systime.sleep(2)  # Wait for order to process
        orderbook = client.orderbook()
        
        if orderbook and "data" in orderbook:
            orders = orderbook["data"]
            
            if isinstance(orders, list) and orders:
                for order in orders:
                    if isinstance(order, str):
                        continue
                    
                    if isinstance(order, dict):
                        order_symbol = order.get("symbol", "")
                        order_status = order.get("status", "")
                        order_id = order.get("orderid", "")
                        
                        symbol_match = symbol and order_symbol == symbol
                        id_match = orderid and order_id == str(orderid)
                        
                        if (symbol_match or id_match) and order_status.upper() in ["COMPLETE", "COMPLETED"]:
                            log(f"✅ Order verified: {order_symbol} | Status: {order_status}")
                            return True
        
        log(f"⚠️ Position verification inconclusive for {symbol}")
        
    except Exception as e:
        log(f"❌ Orderbook check error: {e}")
    
    return False

# -------------------------------------------------------
# Order Placement Functions
# -------------------------------------------------------
def get_option_symbol(strike, option_type):
    """Build option symbol: NIFTY23DEC2525850CE"""
    expiry = get_or_fetch_expiry()
    if not expiry:
        return None
    return f"{SYMBOL}{expiry}{strike}{option_type}"

def place_atm_option(option_type, action):
    """Place ATM option order using optionsorder"""
    try:
        expiry_date = get_or_fetch_expiry()
        if expiry_date is None:
            log("❌ Cannot place order without expiry date")
            return None
        
        log(f"📤 Placing {action} order: {option_type} ATM")
        
        resp = client.optionsorder(
            strategy=STRATEGY,
            underlying="NIFTY",
            exchange=OPTION_ORDER_EXCHANGE,
            expiry_date=expiry_date,
            offset="ATM",
            option_type=option_type,
            action=action,
            quantity=LOT_SIZE,
            pricetype="MARKET",
            product="MIS"
        )

        log(f"📥 ORDER RESPONSE: {resp}")

        if resp.get("status") == "success":
            symbol = resp.get("symbol")
            mode = resp.get("mode", "")
            
            if mode == "analyze":
                log(f"⚠️ ANALYZE MODE - Simulated: {symbol}")
            else:
                log(f"✅ Order placed: {symbol}")
            
            return symbol
        else:
            log(f"❌ Order failed: {resp.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        log(f"❌ Order exception: {e}")
        return None

def place_hedge_option(strike, option_type, action):
    """Place hedge option order using placeorder with explicit strike"""
    try:
        symbol = get_option_symbol(strike, option_type)
        if not symbol:
            log("❌ Cannot build symbol without expiry")
            return None
        
        log(f"📤 Placing {action} order: {symbol}")
        
        resp = client.placeorder(
            strategy=STRATEGY,
            symbol=symbol,
            exchange=OPTION_EXCHANGE,
            action=action,
            price_type="MARKET",
            product="MIS",
            quantity=LOT_SIZE
        )

        log(f"📥 ORDER RESPONSE: {resp}")

        if resp.get("status") == "success":
            mode = resp.get("mode", "")
            
            if mode == "analyze":
                log(f"⚠️ ANALYZE MODE - Simulated: {symbol}")
            else:
                log(f"✅ Order placed: {symbol}")
            
            return symbol
        else:
            log(f"❌ Order failed: {resp.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        log(f"❌ Order exception: {e}")
        return None

# -------------------------------------------------------
# Strategy Functions
# -------------------------------------------------------
def reset_daily_counter():
    global straddle_entry_count
    straddle_entry_count = 0
    log(f"🔄 Daily straddle entry counter reset to zero at {datetime.now(IST)}")

def place_straddle_with_hedges():
    global last_reference_spot, current_straddle_symbols, current_hedge_symbols, straddle_entry_count, position_open
    
    if straddle_entry_count >= MAX_STRADDLES_PER_DAY:
        log(f"⛔ Straddle entry limit ({MAX_STRADDLES_PER_DAY}) reached for today.")
        return
    
    spot = get_spot()
    if spot is None:
        log("❌ Cannot get spot price, skipping straddle placement")
        return
    
    atm_strike = get_atm_strike(spot)
    
    log(f"\n{'='*60}")
    log(f"🎯 PLACING STRADDLE + HEDGES")
    log(f"📊 Spot: {spot:.2f} | ATM: {atm_strike}")
    log(f"{'='*60}\n")
    
    # Place SELL orders for ATM straddle using optionsorder
    log("📍 Step 1/4: Selling ATM CE...")
    straddle_ce = place_atm_option("CE", "SELL")
    
    log("📍 Step 2/4: Selling ATM PE...")
    straddle_pe = place_atm_option("PE", "SELL")
    
    if not straddle_ce or not straddle_pe:
        log("❌ Failed to place straddle orders")
        return
    
    # Calculate hedge strikes
    hedge_ce_strike = atm_strike + HEDGE_DISTANCE
    hedge_pe_strike = atm_strike - HEDGE_DISTANCE
    
    log(f"\n🛡️ Placing hedges:")
    log(f"   CE Hedge: {hedge_ce_strike} (ATM+{HEDGE_DISTANCE})")
    log(f"   PE Hedge: {hedge_pe_strike} (ATM-{HEDGE_DISTANCE})")
    
    # Place BUY orders for hedges using placeorder with explicit strikes
    log("📍 Step 3/4: Buying CE Hedge...")
    hedge_ce = place_hedge_option(hedge_ce_strike, "CE", "BUY")
    
    log("📍 Step 4/4: Buying PE Hedge...")
    hedge_pe = place_hedge_option(hedge_pe_strike, "PE", "BUY")
    
    if not hedge_ce or not hedge_pe:
        log("❌ Failed to place hedge orders")
        return
    
    # Update state
    last_reference_spot = spot
    current_straddle_symbols = [straddle_ce, straddle_pe]
    current_hedge_symbols = [hedge_ce, hedge_pe]
    straddle_entry_count += 1
    position_open = True
    
    log(f"\n✅ STRADDLE + HEDGES PLACED SUCCESSFULLY")
    log(f"{'='*60}")
    log(f"📋 POSITION SUMMARY:")
    log(f"   Straddle (SELL): {straddle_ce}, {straddle_pe}")
    log(f"   Hedges (BUY): {hedge_ce}, {hedge_pe}")
    log(f"   Reference Spot: {last_reference_spot:.2f}")
    log(f"   Entry Count: {straddle_entry_count}/{MAX_STRADDLES_PER_DAY}")
    log(f"{'='*60}\n")

def place_new_straddle():
    """Place only new straddle at current ATM, keep existing hedges"""
    global last_reference_spot, current_straddle_symbols, straddle_entry_count
    
    if straddle_entry_count >= MAX_STRADDLES_PER_DAY:
        log(f"⛔ Straddle entry limit ({MAX_STRADDLES_PER_DAY}) reached for today.")
        return
    
    spot = get_spot()
    if spot is None:
        log("❌ Cannot get spot price, skipping new straddle")
        return
    
    atm_strike = get_atm_strike(spot)
    
    log(f"\n{'='*60}")
    log(f"🔄 PLACING NEW STRADDLE (Hedges unchanged)")
    log(f"📊 Spot: {spot:.2f} | ATM: {atm_strike}")
    log(f"{'='*60}\n")
    
    # Place SELL orders for new straddle
    log("📍 Selling new ATM CE...")
    straddle_ce = place_atm_option("CE", "SELL")
    
    log("📍 Selling new ATM PE...")
    straddle_pe = place_atm_option("PE", "SELL")
    
    if not straddle_ce or not straddle_pe:
        log("❌ Failed to place new straddle orders")
        return
    
    # Update state
    last_reference_spot = spot
    current_straddle_symbols = [straddle_ce, straddle_pe]
    straddle_entry_count += 1
    
    log(f"\n✅ NEW STRADDLE PLACED")
    log(f"{'='*60}")
    log(f"   New Straddle: {straddle_ce}, {straddle_pe}")
    log(f"   Reference Spot: {last_reference_spot:.2f}")
    log(f"   Entry Count: {straddle_entry_count}/{MAX_STRADDLES_PER_DAY}")
    log(f"{'='*60}\n")

def close_straddle_only():
    """Close ONLY the straddle legs, NOT the hedges"""
    if not current_straddle_symbols:
        log("⚠️ No straddle positions to close")
        return
    
    log(f"\n{'='*60}")
    log("🔴 CLOSING STRADDLE ONLY (Hedges remain)")
    log(f"{'='*60}\n")
    
    for sym in current_straddle_symbols:
        try:
            log(f"📍 Buying back (closing) {sym}...")
            resp = client.placeorder(
                strategy=STRATEGY,
                symbol=sym,
                action="BUY",
                exchange=OPTION_EXCHANGE,
                price_type="MARKET",
                product="MIS",
                quantity=LOT_SIZE
            )
            log(f"✅ BUY (EXIT) order for {sym}: {resp.get('status', 'unknown')}")
        except Exception as e:
            log(f"❌ Error closing {sym}: {e}")
    
    log(f"{'='*60}\n")

def close_all_positions():
    """Close both straddle and hedges"""
    global position_open
    
    log(f"\n{'='*60}")
    log("🛑 CLOSING ALL POSITIONS (Straddle + Hedges)")
    log(f"{'='*60}\n")
    
    # Close straddle (BUY back the SELL positions)
    if current_straddle_symbols:
        log("📍 Closing Straddle legs...")
        for sym in current_straddle_symbols:
            try:
                resp = client.placeorder(
                    strategy=STRATEGY,
                    symbol=sym,
                    action="BUY",
                    exchange=OPTION_EXCHANGE,
                    price_type="MARKET",
                    product="MIS",
                    quantity=LOT_SIZE
                )
                log(f"   ✅ BUY (EXIT) straddle {sym}: {resp.get('status', 'unknown')}")
            except Exception as e:
                log(f"   ❌ Error closing straddle {sym}: {e}")
    
    # Close hedges (SELL the BUY positions)
    if current_hedge_symbols:
        log("\n📍 Closing Hedge legs...")
        for sym in current_hedge_symbols:
            try:
                resp = client.placeorder(
                    strategy=STRATEGY,
                    symbol=sym,
                    action="SELL",
                    exchange=OPTION_EXCHANGE,
                    price_type="MARKET",
                    product="MIS",
                    quantity=LOT_SIZE
                )
                log(f"   ✅ SELL (EXIT) hedge {sym}: {resp.get('status', 'unknown')}")
            except Exception as e:
                log(f"   ❌ Error closing hedge {sym}: {e}")
    
    position_open = False
    log(f"\n✅ All positions closed")
    log(f"{'='*60}\n")

def rolling_monitor():
    global last_reference_spot
    
    if last_reference_spot is None:
        return
    
    spot = get_spot()
    if spot is None:
        return
    
    threshold = last_reference_spot * (ROLLING_THRESHOLD_PCT / 100.0)
    move = abs(spot - last_reference_spot)
    
    log(f"📍 Monitor | Spot: {spot:.2f} | Ref: {last_reference_spot:.2f} | Move: {move:.2f} | Threshold: {threshold:.2f}")
    
    if move >= threshold:
        log(f"\n{'='*60}")
        log(f"⚠️ ROLLING TRIGGERED!")
        log(f"{'='*60}")
        log(f"   Spot moved {move:.2f} points (threshold: {threshold:.2f})")
        log(f"   From {last_reference_spot:.2f} to {spot:.2f}")
        log(f"   Movement: {((spot - last_reference_spot) / last_reference_spot * 100):.2f}%")
        log(f"{'='*60}\n")
        
        # Close only straddle, keep hedges
        close_straddle_only()
        
        # Place new straddle only
        systime.sleep(2)  # Brief pause
        place_new_straddle()

def eod_exit():
    log("\n⏰ EOD EXIT TRIGGERED")
    close_all_positions()

# -------------------------------------------------------
# Initialization
# -------------------------------------------------------
log("🚀 Starting Rolling Straddle Bot...")
log(f"{'='*60}")
log(f"📍 Host: {API_HOST}")
log(f"🎯 Strategy: {STRATEGY}")
log(f"📊 Symbol: {SYMBOL} | Lot Size: {LOT_SIZE}")
log(f"🔄 Rolling Threshold: {ROLLING_THRESHOLD_PCT}%")
log(f"🛡️ Hedge Distance: {HEDGE_DISTANCE} points")
log(f"⏰ Entry: {STRADDLE_ENTRY_HOUR:02d}:{STRADDLE_ENTRY_MINUTE:02d} IST")
log(f"⏰ Exit: {SQUAREOFF_HOUR:02d}:{SQUAREOFF_MINUTE:02d} IST")
log(f"📈 Max Straddles/Day: {MAX_STRADDLES_PER_DAY}")
log(f"{'='*60}\n")

# Fetch expiry at startup
current_expiry = fetch_current_expiry()
if current_expiry:
    log(f"✅ Bot initialized with expiry: {current_expiry}\n")
else:
    log("⚠️ Warning: Could not fetch expiry. Will retry when placing orders.\n")

# === Scheduler ===
scheduler = BackgroundScheduler(timezone=IST)

# Reset counter before entry time
scheduler.add_job(
    reset_daily_counter,
    'cron',
    day_of_week='mon-fri',
    hour=STRADDLE_ENTRY_HOUR,
    minute=STRADDLE_ENTRY_MINUTE - 1
)

# Place initial straddle with hedges
scheduler.add_job(
    place_straddle_with_hedges,
    'cron',
    day_of_week='mon-fri',
    hour=STRADDLE_ENTRY_HOUR,
    minute=STRADDLE_ENTRY_MINUTE
)

# EOD exit
scheduler.add_job(
    eod_exit,
    'cron',
    day_of_week='mon-fri',
    hour=SQUAREOFF_HOUR,
    minute=SQUAREOFF_MINUTE
)

scheduler.start()

# -------------------------------------------------------
# Main Loop
# -------------------------------------------------------
log("⏳ Bot is running. Waiting for entry time...")

try:
    while True:
        now = datetime.now(IST).time()
        entry_start = dtime(STRADDLE_ENTRY_HOUR, STRADDLE_ENTRY_MINUTE)
        squareoff_time = dtime(SQUAREOFF_HOUR, SQUAREOFF_MINUTE)
        
        # Rolling monitor runs during straddle session only
        if entry_start < now < squareoff_time and last_reference_spot and position_open:
            rolling_monitor()
        
        systime.sleep(5)
        
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    log("\n👋 Bot stopped by user.")
except Exception as e:
    log(f"❌ CRITICAL ERROR: {e}")
    import traceback
    log(f"   Traceback: {traceback.format_exc()}")
    if position_open:
        log("⚠️ Attempting emergency exit...")
        close_all_positions()
    scheduler.shutdown()