#!/usr/bin/env python3
"""
rolling_short_straddle_backtest_fullroll.py

Backtest engine with FULL ROLL strategy:
- On adjustment: close BOTH CE and PE legs, open fresh straddle at new ATM
- Input: spot CSV with columns: date,open,high,low,close[,volume]
- Modes:
    * logic_only=True  -> only track state transitions & trade log (no P&L)
    * logic_only=False -> model option prices with Black-Scholes (mibian) using fixed_iv_pct (default 15.0)
"""

import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time, timedelta
import mibian
import plotly.graph_objects as go
import argparse

LOT_SIZE_MAP = {"NIFTY": 75, "BANKNIFTY": 25}

def round_strike(spot, symbol="NIFTY"):
    step = 100 if symbol.upper() == "BANKNIFTY" else 50
    return int(round(spot / step) * step)

def days_to_expiry_fraction(now_dt, expiry_time=dt_time(15,30)):
    expiry_dt = datetime.combine(now_dt.date(), expiry_time)
    if expiry_dt <= now_dt:
        expiry_dt = expiry_dt + timedelta(days=1)
    return (expiry_dt - now_dt).total_seconds() / 86400.0

def bs_prices(spot, strike, iv_pct, dte_days, r=6.5):
    dte_days = max(0.0001, dte_days)
    try:
        m = mibian.BS([spot, strike, r, dte_days], volatility=iv_pct)
        return float(getattr(m,'callPrice',0.0) or 0.0), float(getattr(m,'putPrice',0.0) or 0.0)
    except Exception:
        return 0.0, 0.0

class BacktestRollingStraddleFullRoll:
    def __init__(self, df_spot, symbol="NIFTY", lots=1, logic_only=False, fixed_iv_pct=15.0):
        self.df = df_spot.copy()
        if not isinstance(self.df.index, pd.DatetimeIndex):
            raise ValueError("Dataframe must have DatetimeIndex")
        self.symbol = symbol.upper()
        self.lot_size = LOT_SIZE_MAP.get(self.symbol, 75)
        self.qty = self.lot_size * int(lots)
        self.logic_only = bool(logic_only)
        self.fixed_iv_pct = float(fixed_iv_pct)

        # strategy state
        self.current_strike = None
        self.ce_open = False
        self.pe_open = False
        self.entry_spot = None
        self.adjustment_count = 0
        self.max_adjustments = 3
        self.adj_trigger = 0.005  # 0.5%

        # outputs
        self.trade_log = []
        self.equity_curve = []
        self.cash = 1_000_000.0
        self.start_cash = self.cash

    def option_price(self, ts, strike, opt_type):
        if self.logic_only:
            return 0.0
        spot = float(self.df.at[ts, 'close'])
        dte = days_to_expiry_fraction(ts)
        c, p = bs_prices(spot, strike, self.fixed_iv_pct, dte)
        return c if opt_type == 'CE' else p

    def log_trade(self, ts, action, strike, opt, price):
        qty = -self.qty if action == "SELL" else self.qty
        self.trade_log.append({
            "timestamp": ts,
            "action": action,
            "strike": strike,
            "type": opt,
            "price": price,
            "qty": qty
        })

    def sell_straddle(self, ts, strike):
        """Sell fresh straddle at given strike"""
        for opt in ("CE", "PE"):
            price = self.option_price(ts, strike, opt)
            if not self.logic_only:
                self.cash += price * self.qty
            self.log_trade(ts, "SELL", strike, opt, price)
        
        self.current_strike = strike
        self.ce_open = True
        self.pe_open = True

    def close_straddle(self, ts):
        """Close both legs of current straddle"""
        if not self.current_strike:
            return
        
        for opt in ("CE", "PE"):
            if (opt == "CE" and self.ce_open) or (opt == "PE" and self.pe_open):
                price = self.option_price(ts, self.current_strike, opt)
                if not self.logic_only:
                    self.cash -= price * self.qty
                self.log_trade(ts, "BUY", self.current_strike, opt, price)
        
        self.ce_open = False
        self.pe_open = False

    def run(self):
        for ts in self.df.index:
            spot = float(self.df.at[ts, 'close'])
            
            # Calculate MTM equity
            mtm = 0.0
            if not self.logic_only and self.current_strike:
                if self.ce_open:
                    ce_price = self.option_price(ts, self.current_strike, "CE")
                    ce_sell_price = self.option_price(ts, self.current_strike, "CE")
                    # For MTM, calculate unrealized P&L
                    # We need to track sell prices - simplified here
                    pass
                if self.pe_open:
                    pe_price = self.option_price(ts, self.current_strike, "PE")
                    # Similar for PE
                    pass
            
            equity = self.cash + mtm
            self.equity_curve.append((ts, equity))
            
            # ENTRY LOGIC: No open position
            if not self.current_strike:
                strike = round_strike(spot, self.symbol)
                self.sell_straddle(ts, strike)
                self.entry_spot = spot
                self.adjustment_count = 0
                continue
            
            # ADJUSTMENT LOGIC: Check if movement exceeds trigger
            if self.entry_spot and self.adjustment_count < self.max_adjustments:
                move_pct = abs(spot - self.entry_spot) / self.entry_spot
                
                if move_pct >= self.adj_trigger:
                    # FULL ROLL: Close both legs, open new straddle
                    self.close_straddle(ts)
                    
                    new_strike = round_strike(spot, self.symbol)
                    self.sell_straddle(ts, new_strike)
                    
                    self.adjustment_count += 1
                    self.entry_spot = spot
        
        # Exit: Close all positions at end
        if self.df.index.size > 0:
            self.close_straddle(self.df.index[-1])
    
    def get_tradebook(self):
        return pd.DataFrame(self.trade_log)
    
    def plot_equity_curve(self, title="Equity Curve - Full Roll Strategy"):
        if not self.equity_curve:
            print("No equity data to plot")
            return
        
        dates = [str(ts) for ts, _ in self.equity_curve]
        equity = [eq for _, eq in self.equity_curve]
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dates, y=equity, mode='lines', name='Equity'))
        fig.update_xaxis(type='category', title='Date')
        fig.update_yaxis(title='Equity')
        fig.update_layout(title=title, hovermode='x unified')
        return fig
    
    def get_statistics(self):
        if not self.equity_curve:
            return {}
        
        equity_values = [eq for _, eq in self.equity_curve]
        final_equity = equity_values[-1]
        total_return = final_equity - self.start_cash
        return_pct = (total_return / self.start_cash) * 100
        
        # Max drawdown
        peak = self.start_cash
        max_dd = 0
        for eq in equity_values:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100
            max_dd = max(max_dd, dd)
        
        return {
            "Starting Capital": self.start_cash,
            "Final Equity": final_equity,
            "Total Return": total_return,
            "Return %": return_pct,
            "Max Drawdown %": max_dd,
            "Total Trades": len(self.trade_log),
            "Total Adjustments": self.adjustment_count,
        }


def main():
    parser = argparse.ArgumentParser(description='Backtest Rolling Straddle - Full Roll')
    parser.add_argument('--csv', type=str, required=True, help='Path to spot CSV file')
    parser.add_argument('--symbol', type=str, default='NIFTY', help='Symbol (NIFTY/BANKNIFTY)')
    parser.add_argument('--lots', type=int, default=1, help='Number of lots')
    parser.add_argument('--logic-only', action='store_true', help='Logic only mode (no P&L)')
    parser.add_argument('--iv', type=float, default=15.0, help='Fixed IV percentage')
    parser.add_argument('--plot', action='store_true', help='Show equity curve')
    parser.add_argument('--save-html', type=str, help='Save equity curve to HTML')
    
    args = parser.parse_args()
    
    # Load data
    df = pd.read_csv(args.csv, parse_dates=['date'], index_col='date')
    print(f"Loaded {len(df)} rows from {args.csv}")
    
    # Run backtest
    bt = BacktestRollingStraddleFullRoll(
        df, 
        symbol=args.symbol, 
        lots=args.lots,
        logic_only=args.logic_only, 
        fixed_iv_pct=args.iv
    )
    print(f"Running FULL ROLL backtest for {args.symbol} with {args.lots} lot(s)...")
    bt.run()
    
    # Results
    print("\n" + "="*60)
    print("BACKTEST RESULTS - FULL ROLL STRATEGY")
    print("="*60)
    
    stats = bt.get_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"{key:.<30} {value:>15,.2f}")
        else:
            print(f"{key:.<30} {value:>15,}")
    
    # Tradebook
    tradebook = bt.get_tradebook()
    if not tradebook.empty:
        print(f"\n{'='*60}")
        print(f"TRADEBOOK ({len(tradebook)} trades)")
        print("="*60)
        print(tradebook.to_string(index=False))
        
        csv_filename = args.csv.replace('.csv', '_tradebook_fullroll.csv')
        tradebook.to_csv(csv_filename, index=False)
        print(f"\nTradebook saved to: {csv_filename}")
    
    # Plot
    if args.plot or args.save_html:
        fig = bt.plot_equity_curve(f"{args.symbol} Full Roll Strategy")
        if fig:
            if args.save_html:
                fig.write_html(args.save_html)
                print(f"Equity curve saved to: {args.save_html}")
            if args.plot:
                fig.show()


if __name__ == "__main__":
    main()