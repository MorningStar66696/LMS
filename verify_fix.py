import psycopg2
import os
import json
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("SUPABASE_DB_USER")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_HOST = os.getenv("SUPABASE_DB_HOST")
DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")
DB_NAME = os.getenv("SUPABASE_DB_NAME")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=int(DB_PORT)
    )

def verify():
    conn = get_connection()
    cur = conn.cursor()
    
    test_sheet_name = "VerificationSheet123"
    
    print("1. Inserting test record with sheet_name...")
    try:
        cur.execute("""
            INSERT INTO saved_settings
            (user_id, name, sheet_url, mappings, automation_rules, switch_rules,
             trigger_interval, daily_ro, created_by, created_at, sheet_name)
            VALUES (1, 'Verification Test', 'http://example.com', '[]', '[]', '[]',
                    60, 100, 'test@example.com', NOW(), %s)
            RETURNING id
        """, (test_sheet_name,))
        
        new_id = cur.fetchone()[0]
        conn.commit()
        print(f"   Inserted ID: {new_id}")
        
        print("2. Retrieving record to verify sheet_name...")
        cur.execute("""
            SELECT sheet_name FROM saved_settings WHERE id = %s
        """, (new_id,))
        
        fetched_name = cur.fetchone()[0]
        print(f"   Fetched sheet_name: {fetched_name}")
        
        if fetched_name == test_sheet_name:
            print("✅ SUCCESS: sheet_name was saved and retrieved correctly.")
        else:
            print(f"❌ FAILURE: Expected {test_sheet_name}, got {fetched_name}")
            
        print("3. Cleaning up...")
        cur.execute("DELETE FROM saved_settings WHERE id = %s", (new_id,))
        conn.commit()
        print("   Cleanup done.")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    verify()
