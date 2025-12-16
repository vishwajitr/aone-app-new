import time as systime
from datetime import datetime, time as dtime
from apscheduler.schedulers.background import BackgroundScheduler
from openalgo import api
import pytz

print("🔁 OpenAlgo Python Bot with Hedges is running.")

# === USER PARAMETERS ===

STRADDLE_ENTRY_HOUR = 9       # 9 for 9:20 AM
STRADDLE_ENTRY_MINUTE = 20    # 20 for 9:20 AM

SQUAREOFF_HOUR = 15           # 15 for 3:20 PM
SQUAREOFF_MINUTE = 20         # 20 for 3:20 PM

MAX_STRADDLES_PER_DAY = 3     # Daily limit on rolling straddles
ROLLING_THRESHOLD_PCT = 0.4   # Threshold for rolling (in percent, e.g. 0.4 means 0.4%)

HEDGE_DISTANCE = 500          # Hedge distance from ATM (500 points)


SYMBOL = "NIFTY"
EXPIRY = "19JUN25"
EXCHANGE = "NSE_INDEX"
OPTION_EXCHANGE = "NFO"
STRIKE_INTERVAL = 50

API_KEY = "YOU-OPENALGO-APIKEY"
API_HOST = "http://127.0.0.1:5000"

client = api(api_key=API_KEY, host=API_HOST)

def get_atm_strike(spot):
    return int(round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL)

def get_spot():
    quote = client.quotes(symbol=SYMBOL, exchange=EXCHANGE)
    print("Quote:", quote)
    data = quote['data']
    if isinstance(data, list):
        data = data[0]
    return data['ltp']

def get_option_symbol(base, expiry, strike, opttype):
    return f"{base}{expiry}{strike}{opttype}"

# --- State ---
last_reference_spot = None
current_straddle_symbols = []  # Only straddle CE and PE
current_hedge_symbols = []     # Only hedge CE and PE
straddle_entry_count = 0

def reset_daily_counter():
    global straddle_entry_count
    straddle_entry_count = 0
    print(f"Daily straddle entry counter reset to zero at {datetime.now()}")

def place_straddle_with_hedges():
    global last_reference_spot, current_straddle_symbols, current_hedge_symbols, straddle_entry_count
    
    if straddle_entry_count >= MAX_STRADDLES_PER_DAY:
        print(f"Straddle entry limit ({MAX_STRADDLES_PER_DAY}) reached for today.")
        return
    
    spot = get_spot()
    atm_strike = get_atm_strike(spot)
    
    # Straddle symbols (SELL at ATM)
    straddle_ce = get_option_symbol(SYMBOL, EXPIRY, atm_strike, "CE")
    straddle_pe = get_option_symbol(SYMBOL, EXPIRY, atm_strike, "PE")
    
    # Hedge symbols (BUY 500 points away)
    hedge_ce_strike = atm_strike + HEDGE_DISTANCE
    hedge_pe_strike = atm_strike - HEDGE_DISTANCE
    hedge_ce = get_option_symbol(SYMBOL, EXPIRY, hedge_ce_strike, "CE")
    hedge_pe = get_option_symbol(SYMBOL, EXPIRY, hedge_pe_strike, "PE")
    
    print(f"\n{'='*60}")
    print(f"PLACING STRADDLE + HEDGES")
    print(f"Spot: {spot} | ATM: {atm_strike}")
    print(f"Straddle: SELL {straddle_ce} + {straddle_pe}")
    print(f"Hedges: BUY {hedge_ce} + {hedge_pe}")
    print(f"{'='*60}\n")
    
    # Place SELL orders for straddle
    for sym in [straddle_ce, straddle_pe]:
        order = client.placeorder(
            strategy=STRATEGY, symbol=sym, action="SELL",
            exchange=OPTION_EXCHANGE, price_type="MARKET",
            product="MIS", quantity=LOT_SIZE
        )
        print(f"SELL Order placed for {sym}: {order}")
    
    # Place BUY orders for hedges
    for sym in [hedge_ce, hedge_pe]:
        order = client.placeorder(
            strategy=STRATEGY, symbol=sym, action="BUY",
            exchange=OPTION_EXCHANGE, price_type="MARKET",
            product="MIS", quantity=LOT_SIZE
        )
        print(f"BUY Order placed (HEDGE) for {sym}: {order}")
    
    # Update state - keep straddle and hedge symbols separate
    last_reference_spot = spot
    current_straddle_symbols = [straddle_ce, straddle_pe]
    current_hedge_symbols = [hedge_ce, hedge_pe]
    straddle_entry_count += 1
    print(f"Straddle Entry Count updated: {straddle_entry_count}")

def place_new_straddle():
    """Place only new straddle at current ATM, keep existing hedges"""
    global last_reference_spot, current_straddle_symbols, straddle_entry_count
    
    if straddle_entry_count >= MAX_STRADDLES_PER_DAY:
        print(f"Straddle entry limit ({MAX_STRADDLES_PER_DAY}) reached for today.")
        return
    
    spot = get_spot()
    atm_strike = get_atm_strike(spot)
    
    # New straddle symbols (SELL at new ATM)
    straddle_ce = get_option_symbol(SYMBOL, EXPIRY, atm_strike, "CE")
    straddle_pe = get_option_symbol(SYMBOL, EXPIRY, atm_strike, "PE")
    
    print(f"\n{'='*60}")
    print(f"PLACING NEW STRADDLE (Hedges remain unchanged)")
    print(f"Spot: {spot} | ATM: {atm_strike}")
    print(f"Straddle: SELL {straddle_ce} + {straddle_pe}")
    print(f"{'='*60}\n")
    
    # Place SELL orders for new straddle
    for sym in [straddle_ce, straddle_pe]:
        order = client.placeorder(
            strategy=STRATEGY, symbol=sym, action="SELL",
            exchange=OPTION_EXCHANGE, price_type="MARKET",
            product="MIS", quantity=LOT_SIZE
        )
        print(f"SELL Order placed for {sym}: {order}")
    
    # Update state
    last_reference_spot = spot
    current_straddle_symbols = [straddle_ce, straddle_pe]
    straddle_entry_count += 1
    print(f"Straddle Entry Count updated: {straddle_entry_count}")

def close_straddle_only():
    """Close ONLY the straddle legs, NOT the hedges"""
    print(f"\n{'='*60}")
    print("CLOSING STRADDLE ONLY (Hedges remain open)")
    print(f"{'='*60}\n")
    
    for sym in current_straddle_symbols:
        order = client.placeorder(
            strategy=STRATEGY, symbol=sym, action="BUY",
            exchange=OPTION_EXCHANGE, price_type="MARKET",
            product="MIS", quantity=LOT_SIZE
        )
        print(f"BUY Order (EXIT) for straddle {sym}: {order}")

def close_all_positions():
    """Close both straddle and hedges"""
    print(f"\n{'='*60}")
    print("CLOSING ALL POSITIONS (Straddle + Hedges)")
    print(f"{'='*60}\n")
    
    # Close straddle (BUY back the SELL positions)
    for sym in current_straddle_symbols:
        order = client.placeorder(
            strategy=STRATEGY, symbol=sym, action="BUY",
            exchange=OPTION_EXCHANGE, price_type="MARKET",
            product="MIS", quantity=LOT_SIZE
        )
        print(f"BUY Order (EXIT) for straddle {sym}: {order}")
    
    # Close hedges (SELL the BUY positions)
    for sym in current_hedge_symbols:
        order = client.placeorder(
            strategy=STRATEGY, symbol=sym, action="SELL",
            exchange=OPTION_EXCHANGE, price_type="MARKET",
            product="MIS", quantity=LOT_SIZE
        )
        print(f"SELL Order (EXIT) for hedge {sym}: {order}")

def rolling_monitor():
    global last_reference_spot
    spot = get_spot()
    print(f"Spot: {spot}")
    print(f"Last Reference Spot: {last_reference_spot}")
    
    threshold = last_reference_spot * (ROLLING_THRESHOLD_PCT / 100.0)
    
    if abs(spot - last_reference_spot) >= threshold:
        print(f"\n⚠️  ROLLING TRIGGERED: Spot moved to {spot} from ref {last_reference_spot} (Threshold: {threshold})")
        
        # Close only straddle, keep hedges
        close_straddle_only()
        
        # Place new straddle only (hedges remain unchanged)
        place_new_straddle()

def eod_exit():
    print("\n🕒 EOD exit triggered.")
    close_all_positions()

# === Scheduler ===
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
scheduler.add_job(reset_daily_counter, 'cron', day_of_week='mon-fri', hour=STRADDLE_ENTRY_HOUR, minute=STRADDLE_ENTRY_MINUTE)
scheduler.add_job(place_straddle_with_hedges, 'cron', day_of_week='mon-fri', hour=STRADDLE_ENTRY_HOUR, minute=STRADDLE_ENTRY_MINUTE)
scheduler.add_job(eod_exit, 'cron', day_of_week='mon-fri', hour=SQUAREOFF_HOUR, minute=SQUAREOFF_MINUTE)
scheduler.start()

try:
    while True:
        now = datetime.now(pytz.timezone("Asia/Kolkata")).time()
        entry_start = dtime(STRADDLE_ENTRY_HOUR, STRADDLE_ENTRY_MINUTE)
        squareoff_time = dtime(SQUAREOFF_HOUR, SQUAREOFF_MINUTE)
        
        # Rolling monitor runs during straddle session only
        if entry_start < now < squareoff_time and last_reference_spot:
            rolling_monitor()
        
        systime.sleep(5)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    print("\n👋 Bot stopped by user.")