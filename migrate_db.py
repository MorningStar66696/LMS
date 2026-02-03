import psycopg2
import os
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

def migrate():
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        print("Checking if 'sheet_name' column exists in 'saved_settings'...")
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='saved_settings' AND column_name='sheet_name';
        """)
        
        if cur.fetchone():
            print("Column 'sheet_name' already exists. Skipping.")
        else:
            print("Adding 'sheet_name' column...")
            cur.execute("""
                ALTER TABLE saved_settings
                ADD COLUMN sheet_name TEXT;
            """)
            conn.commit()
            print("Column 'sheet_name' added successfully.")
            
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate()
