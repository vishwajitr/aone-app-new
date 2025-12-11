#!/usr/bin/env python3
"""
rolling_short_straddle_prod_fullroll.py

Production-ready Rolling Short Straddle with FULL ROLL strategy:
- On adjustment: close BOTH CE and PE legs, then sell fresh straddle at new ATM
- Uses OpenAlgo SDK for live orders
- Cleaner position tracking (no complex cycle management)
"""

from openalgo import api
import time
from datetime import datetime, time as dt_time
import os
import sys
import traceback

print("🔁 OpenAlgo Rolling Straddle (Full Roll) is running.")

class RollingShortStraddleFullRoll:
    def __init__(
        self,
        api_key=None,
        symbol="NIFTY",
        lots=1,
        host="http://127.0.0.1:5000",
        entry_time_str="09:20",
        exit_time_str="15:15",
        max_adjustments=3,
        adjustment_trigger=0.005,   # 0.5%
    ):
        # API setup
        self.api_key = api_key or os.getenv('OPENALGO_APIKEY')
        if not self.api_key:
            print("Error: API key not provided. Set OPENALGO_APIKEY environment variable.")
            sys.exit(1)

        self.client = api(api_key=self.api_key, host=host)

        # Config
        self.symbol = symbol.upper()
        self.lots = int(lots)
        self.lot_size_map = {"NIFTY": 75, "BANKNIFTY": 25}
        self.lot_size = self.lot_size_map.get(self.symbol, 75)
        self.total_qty_per_leg = self.lot_size * self.lots

        self.entry_time_str = entry_time_str
        self.exit_time_str = exit_time_str

        # Strategy state - simplified for full roll
        self.current_strike = None
        self.ce_open = False
        self.pe_open = False
        self.ce_order_id = None
        self.pe_order_id = None
        self.ce_sell_price = None
        self.pe_sell_price = None
        
        self.entry_spot = None
        self.adjustment_count = 0
        self.max_adjustments = int(max_adjustments)
        self.adjustment_trigger = float(adjustment_trigger)

        # Print init
        print("\n" + "="*60)
        print("ROLLING SHORT STRADDLE - FULL ROLL STRATEGY")
        print("="*60)
        print(f"Symbol: {self.symbol}")
        print(f"Lots: {self.lots} (Quantity per leg: {self.total_qty_per_leg})")
        print(f"Adjustment trigger: {self.adjustment_trigger*100:.2f}%")
        print(f"Max adjustments: {self.max_adjustments}")
        print(f"Strategy: Close BOTH legs on adjustment, open fresh straddle")
        print("="*60 + "\n")

    def get_spot_price(self):
        """Fetch spot price"""
        try:
            exch = "NSE_INDEX" if self.symbol in {"NIFTY", "BANKNIFTY", "NIFTYNXT50", "FINNIFTY", "MIDCPNIFTY"} else "NSE"
            response = self.client.quotes(symbol=self.symbol, exchange=exch)
            print("Quote:", response)
            if response is None:
                return None
            if isinstance(response, dict):
                if 'ltp' in response and response['ltp'] is not None:
                    return float(response['ltp'])
                if 'data' in response and isinstance(response['data'], dict) and 'ltp' in response['data']:
                    return float(response['data']['ltp'])
                for v in response.values():
                    if isinstance(v, dict) and 'ltp' in v and v['ltp'] is not None:
                        return float(v['ltp'])
            return None
        except Exception as e:
            print("Error fetching spot price:", e)
            return None

    def get_atm_strike(self, spot_price):
        step = 100 if self.symbol == "BANKNIFTY" else 50
        return int(round(spot_price / step) * step)

    def safe_place_order(self, strike, option_type, action, quantity=None, price_type="MARKET"):
        """Place order and return {'order_id', 'filled_price'}"""
        qty = quantity if quantity is not None else self.total_qty_per_leg
        try:
            resp = self.client.placeorder(
                strategy="RollingStraddleFullRoll",
                symbol=self.symbol,
                action=action,
                exchange="NFO",
                price_type=price_type,
                product="MIS",
                quantity=str(qty),
                position_size=str(qty),
                strike=str(strike),
                option_type=option_type
            )
        except Exception as e:
            print(f"Error placing order: {e}")
            return {"order_id": None, "filled_price": None}

        if not resp:
            print(f"placeorder returned falsy response: {resp}")
            return {"order_id": None, "filled_price": None}

        order_id = resp.get('orderid') or resp.get('order_id') or resp.get('id')
        if not order_id:
            print(f"placeorder response missing order id: {resp}")
            return {"order_id": None, "filled_price": None}

        print(f"✓ Order placed: {action} {strike}{option_type} qty={qty} orderid={order_id}")
        filled_price = self.fetch_order_filled_price(order_id)
        return {"order_id": order_id, "filled_price": filled_price}

    def fetch_order_filled_price(self, order_id, retries=3, delay=1.0):
        """Best-effort: fetch filled price from order status"""
        try:
            for _ in range(retries):
                try:
                    if hasattr(self.client, "orderstatus"):
                        status = self.client.orderstatus(order_id=order_id)
                    elif hasattr(self.client, "getorder"):
                        status = self.client.getorder(order_id=order_id)
                    else:
                        status = None
                except Exception:
                    status = None

                if status and isinstance(status, dict):
                    for key in ('avg_price', 'avg_executed_price', 'avgexecprice', 'filled_price'):
                        if key in status and status[key]:
                            try:
                                return float(status[key])
                            except Exception:
                                pass
                time.sleep(delay)
            
            # Fallback to spot
            spot = self.get_spot_price()
            return float(spot) if spot is not None else None
        except Exception as e:
            print(f"Error in fetch_order_filled_price: {e}")
            return None

    def sell_straddle(self, strike):
        """Sell fresh straddle at given strike"""
        self.log(f"SELLING STRADDLE at strike {strike}")
        
        # Sell CE
        res_ce = self.safe_place_order(strike=strike, option_type="CE", action="SELL")
        time.sleep(0.5)
        
        # Sell PE
        res_pe = self.safe_place_order(strike=strike, option_type="PE", action="SELL")
        time.sleep(0.5)
        
        # Update state
        self.current_strike = strike
        self.ce_open = bool(res_ce["order_id"])
        self.pe_open = bool(res_pe["order_id"])
        self.ce_order_id = res_ce["order_id"]
        self.pe_order_id = res_pe["order_id"]
        self.ce_sell_price = res_ce["filled_price"]
        self.pe_sell_price = res_pe["filled_price"]
        
        self.log(f"Straddle sold: CE open={self.ce_open}, PE open={self.pe_open}")

    def close_straddle(self):
        """Close both legs of current straddle"""
        if not self.current_strike:
            self.log("No straddle to close")
            return
        
        self.log(f"CLOSING STRADDLE at strike {self.current_strike}")
        
        ce_pnl = None
        pe_pnl = None
        
        # Close CE if open
        if self.ce_open:
            res_ce = self.safe_place_order(strike=self.current_strike, option_type="CE", action="BUY")
            if res_ce["order_id"]:
                buy_price = res_ce["filled_price"]
                if self.ce_sell_price and buy_price:
                    ce_pnl = (self.ce_sell_price - buy_price) * self.total_qty_per_leg
                    self.log(f"CE closed: sell={self.ce_sell_price:.2f} buy={buy_price:.2f} pnl={ce_pnl:.2f}")
                self.ce_open = False
            time.sleep(0.5)
        
        # Close PE if open
        if self.pe_open:
            res_pe = self.safe_place_order(strike=self.current_strike, option_type="PE", action="BUY")
            if res_pe["order_id"]:
                buy_price = res_pe["filled_price"]
                if self.pe_sell_price and buy_price:
                    pe_pnl = (self.pe_sell_price - buy_price) * self.total_qty_per_leg
                    self.log(f"PE closed: sell={self.pe_sell_price:.2f} buy={buy_price:.2f} pnl={pe_pnl:.2f}")
                self.pe_open = False
            time.sleep(0.5)
        
        # Log total P&L for this straddle
        if ce_pnl is not None and pe_pnl is not None:
            total_pnl = ce_pnl + pe_pnl
            self.log(f"Straddle P&L: CE={ce_pnl:.2f} PE={pe_pnl:.2f} Total={total_pnl:.2f}")
        
        # Reset state
        self.current_strike = None
        self.ce_order_id = None
        self.pe_order_id = None
        self.ce_sell_price = None
        self.pe_sell_price = None

    def log(self, txt):
        print(f"{datetime.now().isoformat()} | {txt}")

    def initial_entry(self):
        """Initial entry: sell straddle at ATM"""
        spot = self.get_spot_price()
        if spot is None:
            self.log("Failed to fetch spot for entry")
            return False
        
        atm = self.get_atm_strike(spot)
        self.entry_spot = spot
        self.log(f"INITIAL ENTRY: spot={spot:.2f} ATM={atm}")
        
        self.sell_straddle(atm)
        return True

    def check_adjustment_trigger(self, current_spot):
        """Check if adjustment is needed"""
        if self.entry_spot is None:
            return False, None
        
        move_pct = (current_spot - self.entry_spot) / self.entry_spot
        if abs(move_pct) >= self.adjustment_trigger:
            return True, ("UP" if move_pct > 0 else "DOWN")
        
        return False, None

    def execute_adjustment(self, direction, current_spot):
        """Execute FULL ROLL: close both legs, open new straddle"""
        if self.adjustment_count >= self.max_adjustments:
            self.log("Max adjustments reached; skipping")
            return False
        
        self.log(f"ADJUSTMENT #{self.adjustment_count+1} triggered: direction={direction}")
        self.log("FULL ROLL: Closing BOTH legs")
        
        # Close entire straddle
        self.close_straddle()
        
        # Open new straddle at new ATM
        new_atm = self.get_atm_strike(current_spot)
        self.sell_straddle(new_atm)
        
        # Update counters
        self.adjustment_count += 1
        self.entry_spot = current_spot
        
        self.log(f"Adjustment #{self.adjustment_count} completed. New strike={new_atm}")
        return True

    def run(self):
        """Main strategy loop"""
        try:
            # Wait for entry time
            self.log(f"Waiting for entry time {self.entry_time_str}...")
            while True:
                now = datetime.now()
                cur_time = now.strftime("%H:%M")
                
                if cur_time == self.entry_time_str and not self.entry_spot:
                    ok = self.initial_entry()
                    if ok:
                        break
                    else:
                        self.log("Entry failed; retrying in 30s")
                        time.sleep(30)
                
                time.sleep(5)
            
            # Monitor loop
            self.log("Entry completed. Starting monitor loop.")
            while True:
                now = datetime.now()
                cur_time = now.strftime("%H:%M")
                
                # Exit time check
                if cur_time >= self.exit_time_str:
                    self.log("Exit time reached. Closing all positions.")
                    self.close_straddle()
                    self.log(f"Total adjustments: {self.adjustment_count}")
                    break
                
                # Get current spot
                spot = self.get_spot_price()
                if spot is None:
                    time.sleep(10)
                    continue
                
                # Calculate move from entry
                move_pct = ((spot - self.entry_spot) / self.entry_spot * 100) if self.entry_spot else 0.0
                self.log(f"Spot: {spot:.2f} | Entry: {self.entry_spot:.2f} | Move: {move_pct:.3f}% | Adj: {self.adjustment_count}/{self.max_adjustments}")
                
                # Check adjustment trigger
                trigger, direction = self.check_adjustment_trigger(spot)
                if trigger and self.adjustment_count < self.max_adjustments:
                    self.execute_adjustment(direction, spot)
                    time.sleep(60)  # Wait 1 min after adjustment
                    continue
                
                time.sleep(15)  # Regular monitoring interval
            
            self.log("Strategy run finished.")
        
        except KeyboardInterrupt:
            self.log("Interrupted by user.")
            ans = input("Close all positions? (y/n): ").strip().lower()
            if ans == 'y':
                self.close_straddle()
        
        except Exception as e:
            print(f"Strategy error: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    API_KEY = os.getenv('OPENALGO_APIKEY')
    SYMBOL = "NIFTY"
    LOTS = 1
    HOST = os.getenv('OPENALGO_API_HOST', 'http://127.0.0.1:5000')

    strat = RollingShortStraddleFullRoll(
        api_key=API_KEY,
        symbol=SYMBOL,
        lots=LOTS,
        host=HOST,
        entry_time_str="09:20",
        exit_time_str="15:15",
        max_adjustments=3,
        adjustment_trigger=0.005  # 0.5%
    )
    strat.run()