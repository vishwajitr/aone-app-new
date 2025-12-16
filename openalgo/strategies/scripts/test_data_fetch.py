#!/usr/bin/env python3
"""
Test data fetching from OpenAlgo API
This helps debug what format the API returns
"""

from openalgo import api
import os
import pandas as pd

# Get API key
api_key = os.getenv('OPENALGO_APIKEY')
if not api_key:
    print("❌ Error: OPENALGO_APIKEY environment variable not set")
    print("\nSet it with:")
    print("export OPENALGO_APIKEY='your_api_key_here'")
    exit(1)

print(f"✅ API Key found: {api_key[:10]}...")

# Initialize client
print("\n📡 Initializing OpenAlgo client...")
client = api(api_key=api_key, host="http://127.0.0.1:5000")
print("✅ Client initialized")

# Test data fetch
print("\n📊 Fetching test data...")
print("Symbol: NIFTY")
print("Exchange: NSE")
print("Interval: 1m")
print("Date: 2024-12-05 to 2024-12-06")

try:
    response = client.history(
        symbol="NIFTY",
        exchange="NSE",
        interval="1m",
        start_date="2024-12-05",
        end_date="2024-12-06"
    )
    
    print(f"\n🔍 RESPONSE ANALYSIS")
    print(f"{'='*70}")
    print(f"Type: {type(response)}")
    print(f"{'='*70}")
    
    if isinstance(response, dict):
        print("\n📋 Dictionary Keys:")
        for key in response.keys():
            print(f"  - {key}")
        
        if 'status' in response:
            print(f"\nStatus: {response['status']}")
        
        if 'message' in response:
            print(f"Message: {response['message']}")
        
        if 'data' in response:
            data = response['data']
            print(f"\nData Type: {type(data)}")
            
            if isinstance(data, list):
                print(f"Data Length: {len(data)} items")
                if len(data) > 0:
                    print(f"\nFirst Item:")
                    print(f"  Type: {type(data[0])}")
                    if isinstance(data[0], dict):
                        print(f"  Keys: {list(data[0].keys())}")
                        print(f"  Sample: {data[0]}")
                    
                    print(f"\nLast Item:")
                    print(f"  {data[-1]}")
            
            elif isinstance(data, pd.DataFrame):
                print(f"\nDataFrame Info:")
                print(f"  Shape: {data.shape}")
                print(f"  Columns: {data.columns.tolist()}")
                print(f"\nFirst 3 rows:")
                print(data.head(3))
    
    elif isinstance(response, pd.DataFrame):
        print("\n✅ Response is a DataFrame!")
        print(f"Shape: {response.shape}")
        print(f"Columns: {response.columns.tolist()}")
        print("\nFirst 3 rows:")
        print(response.head(3))
        print("\nLast 3 rows:")
        print(response.tail(3))
    
    else:
        print(f"\n⚠️  Unexpected response type!")
        print(f"Response: {str(response)[:500]}")
    
    print(f"\n{'='*70}")
    print("✅ Test completed!")
    
except Exception as e:
    print(f"\n❌ Error occurred:")
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()













