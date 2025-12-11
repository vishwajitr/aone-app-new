#!/usr/bin/env python3
"""
Test OpenAlgo API directly with requests
"""

import requests
import os
import json

# Get API key
api_key = os.getenv('OPENALGO_APIKEY')
if not api_key:
    print("❌ Error: OPENALGO_APIKEY environment variable not set")
    print("\nSet it with:")
    print("export OPENALGO_APIKEY='your_api_key_here'")
    exit(1)

print(f"✅ API Key found: {api_key[:10]}...")

# API endpoint
url = "http://127.0.0.1:5000/api/v1/history"

# Request payload
payload = {
    "apikey": api_key,
    "symbol": "NIFTY",
    "exchange": "NSE",
    "interval": "1m",
    "start_date": "2024-12-05",
    "end_date": "2024-12-06"
}

print("\n📡 Calling OpenAlgo API...")
print(f"URL: {url}")
print(f"Payload: {json.dumps(payload, indent=2)}")

try:
    response = requests.post(url, json=payload)
    
    print(f"\n{'='*70}")
    print("RESPONSE ANALYSIS")
    print(f"{'='*70}")
    print(f"Status Code: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    if response.status_code == 200:
        print("\n✅ Request successful!")
        
        # Try to parse as JSON
        try:
            data = response.json()
            print(f"\nResponse Type: {type(data)}")
            print(f"\nResponse Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            print(f"\nFull Response (formatted):")
            print(json.dumps(data, indent=2)[:1000])
            print("\n... (truncated)")
            
            # Analyze data structure
            if isinstance(data, dict):
                if 'status' in data:
                    print(f"\n📊 Status: {data['status']}")
                if 'data' in data:
                    print(f"📊 Data Type: {type(data['data'])}")
                    if isinstance(data['data'], list) and len(data['data']) > 0:
                        print(f"📊 Data Length: {len(data['data'])}")
                        print(f"📊 First Item Keys: {list(data['data'][0].keys())}")
                        print(f"\n📝 First 3 Items:")
                        for i, item in enumerate(data['data'][:3]):
                            print(f"  {i+1}. {item}")
        
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON: {e}")
            print(f"Raw response: {response.text[:500]}")
    
    else:
        print(f"\n❌ Request failed!")
        print(f"Response: {response.text}")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()









