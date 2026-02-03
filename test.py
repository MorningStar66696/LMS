# import psycopg2
# from dotenv import load_dotenv
# import os

# load_dotenv()

# DB_USER = os.getenv("SUPABASE_DB_USER")
# DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
# DB_HOST = os.getenv("SUPABASE_DB_HOST")
# DB_PORT = os.getenv("SUPABASE_DB_PORT")
# DB_NAME = os.getenv("SUPABASE_DB_NAME")

# print("DEBUG ENV:")
# print("DB_USER =", DB_USER)
# print("DB_HOST =", DB_HOST)
# print("DB_PORT =", DB_PORT)
# print("DB_NAME =", DB_NAME)

# try:
#     connection = psycopg2.connect(
#         user=DB_USER,
#         password=DB_PASSWORD,
#         host=DB_HOST,
#         port=int(DB_PORT),
#         dbname=DB_NAME,
#         connect_timeout=10
#     )

#     print("✅ Connection successful!")

#     cursor = connection.cursor()
#     cursor.execute("SELECT NOW();")
#     print("🕒 Server time:", cursor.fetchone())

#     cursor.close()
#     connection.close()
#     print("✅ Connection closed.")

# except Exception as e:
#     print("❌ Failed to connect:", e)