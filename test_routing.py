import requests
import json
import os
from dotenv import load_dotenv

# URL from your app.py
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzeorj-ZaDZdxcbfCVw36PpxnblVQfrfXc4eQphfDVGBn9T_0vXK_tEkYjaDSvGqSLm/exec"

def test_routing():
    print(f"🔥 Testing Google Script URL: {GOOGLE_SCRIPT_URL}\n")

    # TEST 1: Send to Sheet1 (or whatever your first sheet is)
    print("--- TEST 1: Sending to 'Sheet1' ---")
    payload1 = {
        "sheetUrl": "https://docs.google.com/spreadsheets/d/1u3ePZ8pGR8IsX6s3v0waSHhDry3-1a_aaLoP_GPo-MY/edit",
        "sheetName": "Sheet1",
        "rows": [{"TestColumn": "Value1", "Source": "PythonScript_Sheet1"}],
        "mappings": [] # Simulating raw append
    }
    
    try:
        r1 = requests.post(GOOGLE_SCRIPT_URL, json=payload1, timeout=30)
        print(f"Response: {r1.text}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "="*50 + "\n")

    # TEST 2: Send to Sheet2
    print("--- TEST 2: Sending to 'Sheet2' ---")
    payload2 = {
        "sheetUrl": "https://docs.google.com/spreadsheets/d/1u3ePZ8pGR8IsX6s3v0waSHhDry3-1a_aaLoP_GPo-MY/edit",
        "sheetName": "Sheet2",
        "rows": [{"TestColumn": "Value2", "Source": "PythonScript_Sheet2"}],
        "mappings": []
    }

    try:
        r2 = requests.post(GOOGLE_SCRIPT_URL, json=payload2, timeout=30)
        print(f"Response: {r2.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_routing()
