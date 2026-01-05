"""
Helper script to download and save S&P 500 ticker list.
Run this once to create sp500_tickers.txt for the main tracker.
"""

import pandas as pd
import requests
from pathlib import Path

def download_sp500_tickers(save_dir="sp500_momentum_data"):
    """Download S&P 500 ticker list and save to file."""
    
    save_path = Path(save_dir)
    save_path.mkdir(exist_ok=True)
    output_file = save_path / "sp500_tickers.txt"
    
    methods = [
        ("Wikipedia with requests", download_from_wikipedia_requests),
        ("Wikipedia with pandas", download_from_wikipedia_pandas),
        ("Alternative source", download_from_alternative)
    ]
    
    for method_name, method_func in methods:
        print(f"\nTrying method: {method_name}...")
        try:
            tickers = method_func()
            if tickers:
                # Save to file
                with open(output_file, 'w') as f:
                    for ticker in tickers:
                        f.write(f"{ticker}\n")
                
                print(f"✓ Success! Saved {len(tickers)} tickers to {output_file}")
                print(f"\nFirst 10 tickers: {', '.join(tickers[:10])}")
                print(f"Last 10 tickers: {', '.join(tickers[-10:])}")
                return tickers
        except Exception as e:
            print(f"✗ Failed: {e}")
    
    print("\n❌ All methods failed. Creating file with major tickers only...")
    create_fallback_file(output_file)
    return None

def download_from_wikipedia_requests():
    """Method 1: Use requests with headers."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    
    tables = pd.read_html(response.text)
    sp500_table = tables[0]
    tickers = sp500_table['Symbol'].tolist()
    # Clean tickers for Yahoo Finance format
    tickers = [ticker.replace('.', '-') for ticker in tickers]
    return tickers

def download_from_wikipedia_pandas():
    """Method 2: Direct pandas read_html."""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    sp500_table = tables[0]
    tickers = sp500_table['Symbol'].tolist()
    tickers = [ticker.replace('.', '-') for ticker in tickers]
    return tickers

def download_from_alternative():
    """Method 3: Alternative data source (SlickCharts mirror)."""
    # This is a community-maintained list
    url = 'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv'
    df = pd.read_csv(url)
    tickers = df['Symbol'].tolist()
    tickers = [ticker.replace('.', '-') for ticker in tickers]
    return tickers

def create_fallback_file(output_file):
    """Create file with major S&P 500 companies as fallback."""
    major_tickers = [
        # Technology
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO', 'ORCL', 'ADBE',
        'CRM', 'ACN', 'AMD', 'CSCO', 'INTC', 'IBM', 'QCOM', 'TXN', 'INTU', 'NOW',
        
        # Healthcare
        'UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'PFE', 'BMY',
        'AMGN', 'GILD', 'CVS', 'CI', 'ISRG', 'REGN', 'VRTX', 'MDT', 'BSX', 'ELV',
        
        # Financials
        'JPM', 'BAC', 'WFC', 'MS', 'GS', 'BLK', 'SPGI', 'C', 'AXP', 'USB',
        'PNC', 'TFC', 'BK', 'SCHW', 'COF', 'MMC', 'AON', 'ICE', 'MCO', 'CME',
        
        # Consumer Discretionary
        'AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'SBUX', 'TJX', 'LOW', 'BKNG', 'ABNB',
        
        # Consumer Staples
        'WMT', 'PG', 'COST', 'KO', 'PEP', 'PM', 'MO', 'CL', 'MDLZ', 'KMB',
        
        # Energy
        'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO', 'OXY', 'HAL',
        
        # Industrials
        'UPS', 'RTX', 'HON', 'UNP', 'BA', 'CAT', 'GE', 'DE', 'LMT', 'MMM',
        
        # Communication Services
        'META', 'GOOGL', 'NFLX', 'DIS', 'CMCSA', 'VZ', 'T', 'TMUS', 'EA', 'PARA',
        
        # Utilities
        'NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'SRE', 'PEG', 'XEL', 'ED',
        
        # Real Estate
        'PLD', 'AMT', 'EQIX', 'PSA', 'SPG', 'WELL', 'DLR', 'O', 'VICI', 'AVB',
        
        # Materials
        'LIN', 'APD', 'SHW', 'ECL', 'FCX', 'NEM', 'DOW', 'DD', 'NUE', 'VMC'
    ]
    
    # Remove duplicates and sort
    major_tickers = sorted(list(set(major_tickers)))
    
    with open(output_file, 'w') as f:
        for ticker in major_tickers:
            f.write(f"{ticker}\n")
    
    print(f"✓ Created fallback file with {len(major_tickers)} major S&P 500 tickers")
    print("⚠️  This is NOT the complete S&P 500 list!")
    print("   Try running this script again later to get the full list.")

if __name__ == "__main__":
    print("=" * 70)
    print("S&P 500 Ticker List Downloader")
    print("=" * 70)
    
    tickers = download_sp500_tickers()
    
    if tickers:
        print("\n✓ Setup complete! You can now run the main tracker script.")
    else:
        print("\n⚠️  Using fallback list. Your tracker will work but won't scan all 500 stocks.")
        print("   This is fine for testing, but try to get the full list later.")
    
    print("\n" + "=" * 70)