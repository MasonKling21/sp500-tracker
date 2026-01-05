import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
from pathlib import Path

class SP500MomentumTracker:
    """
    Tracks S&P 500 stocks with >5% daily gains/losses and monitors them for 10 business days.
    Designed to run daily at market close (Monday-Friday).
    """
    
    def __init__(self, data_dir="sp500_momentum_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # File paths
        self.tracking_file = self.data_dir / "tracking_list.csv"
        self.historical_file = self.data_dir / "historical_tracking.csv"
        self.daily_snapshot_file = self.data_dir / "daily_snapshots.csv"
        
    def get_sp500_tickers(self):
        """Fetch current S&P 500 ticker list with multiple fallback methods."""
        # Method 1: Try Wikipedia with headers
        try:
            import requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            response = requests.get(url, headers=headers)
            tables = pd.read_html(response.text)
            sp500_table = tables[0]
            tickers = sp500_table['Symbol'].tolist()
            tickers = [ticker.replace('.', '-') for ticker in tickers]
            print(f"Successfully fetched {len(tickers)} S&P 500 tickers from Wikipedia")
            return tickers
        except Exception as e:
            print(f"Wikipedia method failed: {e}")
        
        # Method 2: Use a cached/static list file
        cache_file = self.data_dir / "sp500_tickers.txt"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    tickers = [line.strip() for line in f if line.strip()]
                print(f"Loaded {len(tickers)} tickers from cache file")
                return tickers
            except Exception as e:
                print(f"Cache file method failed: {e}")
        
        # Method 3: Fallback to a smaller representative list
        print("WARNING: Using fallback ticker list. Update sp500_tickers.txt with full list!")
        fallback_tickers = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B',
            'UNH', 'JNJ', 'XOM', 'V', 'PG', 'JPM', 'MA', 'HD', 'CVX', 'MRK',
            'LLY', 'ABBV', 'PEP', 'AVGO', 'KO', 'COST', 'WMT', 'MCD', 'CSCO',
            'TMO', 'ACN', 'ABT', 'ADBE', 'CRM', 'NFLX', 'DHR', 'VZ', 'NKE',
            'TXN', 'DIS', 'INTC', 'CMCSA', 'PM', 'UPS', 'ORCL', 'AMD', 'NEE'
        ]
        return fallback_ickers
    
    def get_daily_movers(self, threshold=5.0):
        """
        Find stocks with >threshold% gains or losses today.
        
        Returns:
            DataFrame with columns: ticker, date, change_pct, close_price
        """
        tickers = self.get_sp500_tickers()
        movers = []
        
        print(f"Scanning {len(tickers)} S&P 500 stocks for >{threshold}% moves...")
        
        for i, ticker in enumerate(tickers):
            try:
                stock = yf.Ticker(ticker)
                # Get last 2 days of data
                hist = stock.history(period="5d")
                
                if len(hist) < 2:
                    continue
                
                # Calculate daily change
                today_close = hist['Close'].iloc[-1]
                prev_close = hist['Close'].iloc[-2]
                change_pct = ((today_close - prev_close) / prev_close) * 100
                
                if abs(change_pct) >= threshold:
                    movers.append({
                        'ticker': ticker,
                        'date_detected': datetime.now().strftime('%Y-%m-%d'),
                        'detection_price': today_close,
                        'change_pct': round(change_pct, 2),
                        'business_days_tracked': 0,
                        'status': 'active'
                    })
                    print(f"  Found: {ticker} ({change_pct:+.2f}%)")
                
                # Rate limiting
                if (i + 1) % 50 == 0:
                    print(f"  Progress: {i + 1}/{len(tickers)}")
                    time.sleep(1)
                    
            except Exception as e:
                print(f"  Error processing {ticker}: {e}")
                continue
        
        return pd.DataFrame(movers)
    
    def update_tracking_list(self, new_movers):
        """Add new movers to tracking list and update existing entries."""
        # Load existing tracking list
        if self.tracking_file.exists():
            tracking_df = pd.read_csv(self.tracking_file)
        else:
            tracking_df = pd.DataFrame()
        
        # Add new movers
        if not new_movers.empty:
            tracking_df = pd.concat([tracking_df, new_movers], ignore_index=True)
            print(f"Added {len(new_movers)} new stocks to tracking list")
        
        # Save updated list
        if not tracking_df.empty:
            tracking_df.to_csv(self.tracking_file, index=False)
        
        return tracking_df
    
    def get_current_prices(self, tickers):
        """Fetch current prices for a list of tickers."""
        prices = {}
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="1d")
                if not hist.empty:
                    prices[ticker] = hist['Close'].iloc[-1]
            except Exception as e:
                print(f"  Error fetching price for {ticker}: {e}")
                prices[ticker] = None
        return prices
    
    def track_existing_stocks(self):
        """Update tracking for stocks already being monitored."""
        if not self.tracking_file.exists():
            print("No stocks currently being tracked.")
            return pd.DataFrame()
        
        tracking_df = pd.read_csv(self.tracking_file)
        active_stocks = tracking_df[tracking_df['status'] == 'active']
        
        if active_stocks.empty:
            print("No active stocks to track.")
            return pd.DataFrame()
        
        print(f"\nUpdating {len(active_stocks)} actively tracked stocks...")
        
        # Get current prices
        current_prices = self.get_current_prices(active_stocks['ticker'].tolist())
        
        daily_updates = []
        
        for idx, row in active_stocks.iterrows():
            ticker = row['ticker']
            current_price = current_prices.get(ticker)
            
            if current_price is None:
                continue
            
            # Calculate metrics
            days_tracked = row['business_days_tracked'] + 1
            price_change = ((current_price - row['detection_price']) / row['detection_price']) * 100
            
            # Update tracking data
            tracking_df.loc[idx, 'business_days_tracked'] = days_tracked
            tracking_df.loc[idx, 'current_price'] = current_price
            tracking_df.loc[idx, 'total_change_pct'] = round(price_change, 2)
            tracking_df.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d')
            
            # Mark as complete after 10 business days
            if days_tracked >= 10:
                tracking_df.loc[idx, 'status'] = 'completed'
                print(f"  {ticker}: Completed 10-day tracking ({price_change:+.2f}% total)")
            else:
                print(f"  {ticker}: Day {days_tracked}/10 ({price_change:+.2f}% total)")
            
            # Record daily snapshot
            daily_updates.append({
                'ticker': ticker,
                'date_detected': row['date_detected'],
                'tracking_day': days_tracked,
                'current_date': datetime.now().strftime('%Y-%m-%d'),
                'detection_price': row['detection_price'],
                'current_price': current_price,
                'initial_change_pct': row['change_pct'],
                'cumulative_change_pct': round(price_change, 2),
                'status': tracking_df.loc[idx, 'status']
            })
        
        # Save updated tracking list
        tracking_df.to_csv(self.tracking_file, index=False)
        
        # Append to daily snapshots
        if daily_updates:
            daily_df = pd.DataFrame(daily_updates)
            if self.daily_snapshot_file.exists():
                daily_df.to_csv(self.daily_snapshot_file, mode='a', header=False, index=False)
            else:
                daily_df.to_csv(self.daily_snapshot_file, index=False)
        
        return pd.DataFrame(daily_updates)
    
    def archive_completed_stocks(self):
        """Move completed tracking records to historical file."""
        if not self.tracking_file.exists():
            return
        
        tracking_df = pd.read_csv(self.tracking_file)
        completed = tracking_df[tracking_df['status'] == 'completed']
        
        if completed.empty:
            return
        
        # Append to historical file
        if self.historical_file.exists():
            completed.to_csv(self.historical_file, mode='a', header=False, index=False)
        else:
            completed.to_csv(self.historical_file, index=False)
        
        # Remove from active tracking
        tracking_df = tracking_df[tracking_df['status'] == 'active']
        tracking_df.to_csv(self.tracking_file, index=False)
        
        print(f"\nArchived {len(completed)} completed stocks to historical file")
    
    def run_daily_update(self, threshold=5.0):
        """Main function to run daily - scans for new movers and updates existing tracking."""
        print("=" * 70)
        print(f"S&P 500 Momentum Tracker - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Step 1: Find new movers
        print("\n[1/4] Scanning for new significant movers...")
        new_movers = self.get_daily_movers(threshold=threshold)
        
        # Step 2: Update tracking list
        print("\n[2/4] Updating tracking list...")
        self.update_tracking_list(new_movers)
        
        # Step 3: Track existing stocks
        print("\n[3/4] Tracking existing stocks...")
        daily_updates = self.track_existing_stocks()
        
        # Step 4: Archive completed stocks
        print("\n[4/4] Archiving completed stocks...")
        self.archive_completed_stocks()
        
        print("\n" + "=" * 70)
        print("Daily update completed successfully!")
        print(f"New movers found: {len(new_movers)}")
        print(f"Data directory: {self.data_dir.absolute()}")
        print("=" * 70)
        
        return {
            'new_movers': new_movers,
            'daily_updates': daily_updates
        }
    
    def get_ml_ready_dataset(self):
        """
        Prepare ML-ready dataset from daily snapshots.
        
        Returns:
            DataFrame with features for ML modeling
        """
        if not self.daily_snapshot_file.exists():
            print("No snapshot data available yet.")
            return pd.DataFrame()
        
        df = pd.read_csv(self.daily_snapshot_file)
        
        # Create additional features
        df['date_detected'] = pd.to_datetime(df['date_detected'])
        df['current_date'] = pd.to_datetime(df['current_date'])
        df['days_since_detection'] = (df['current_date'] - df['date_detected']).dt.days
        
        # Feature: Initial move direction
        df['initial_move_direction'] = df['initial_change_pct'].apply(lambda x: 'gain' if x > 0 else 'loss')
        
        # Feature: Magnitude of initial move
        df['initial_move_magnitude'] = df['initial_change_pct'].abs()
        
        # Feature: Price momentum (current vs initial)
        df['momentum_continuation'] = (df['cumulative_change_pct'] / df['initial_change_pct'])
        
        # Target variable: Did the stock continue in the same direction?
        df['continued_direction'] = ((df['initial_change_pct'] > 0) & (df['cumulative_change_pct'] > 0)) | \
                                    ((df['initial_change_pct'] < 0) & (df['cumulative_change_pct'] < 0))
        
        return df


# Usage example
if __name__ == "__main__":
    # Initialize tracker
    tracker = SP500MomentumTracker(data_dir="sp500_momentum_data")
    
    # Run daily update (run this script once per day at market close)
    results = tracker.run_daily_update(threshold=5.0)
    
    # Optional: Get ML-ready dataset
    # ml_data = tracker.get_ml_ready_dataset()
    # print(f"\nML dataset shape: {ml_data.shape}")
    # ml_data.to_csv("ml_ready_dataset.csv", index=False)