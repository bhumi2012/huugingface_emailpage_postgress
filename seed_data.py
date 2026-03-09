"""
seed_data.py — Creates the database/table if needed, then inserts sample encrypted rows.
Run this once before running app.py to populate test data.
"""

import psycopg2
import os
import sys
import logging
from cryptography.fernet import Fernet
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("seed_data")

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("APP_DB", os.getenv("POSTGRES_DB", "encrypted_db"))
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not DB_USER or not DB_PASSWORD or not DB_NAME or not ENCRYPTION_KEY:
    logger.error("❌ Missing required environment variables")
    sys.exit(1)

cipher = Fernet(ENCRYPTION_KEY.encode())

# --------------------------------
# Ensure Database Exists
# --------------------------------

logger.info(f"Checking if database '{DB_NAME}' exists...")
admin_conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database="postgres",
    user=DB_USER,
    password=DB_PASSWORD
)
admin_conn.autocommit = True
admin_cur = admin_conn.cursor()
admin_cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
if admin_cur.fetchone() is None:
    admin_cur.execute(f'CREATE DATABASE "{DB_NAME}"')
    logger.info(f"✅ Database '{DB_NAME}' created")
else:
    logger.info(f"✅ Database '{DB_NAME}' already exists")
admin_cur.close()
admin_conn.close()

# --------------------------------
# Connect to Target Database
# --------------------------------

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cur = conn.cursor()
logger.info(f"✅ Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")

# Create table
cur.execute("""
    CREATE TABLE IF NOT EXISTS secure_data (
        id SERIAL PRIMARY KEY,
        encrypted_text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()
logger.info("✅ Table 'secure_data' ready.")

# Sample data to encrypt and insert
sample_texts = [
    "john.doe@example.com",
    "jane.smith@company.org",
    "Order #12345 — shipped to New York",
    "Patient record: blood pressure 120/80",
    "Credit card ending in 4242",
    "Meeting notes: discuss Q3 budget",
    "API key: sk-test-abc123xyz",
    "Employee SSN: 123-45-6789",
    "Invoice #9876 for $1,500.00",
    "Confidential: merger with Acme Corp",
    "alice.wonder@gmail.com",
    "Password reset requested for admin@site.com",
    "Shipping address: 742 Evergreen Terrace",
    "Medical diagnosis: Type 2 Diabetes",
    "Contract signed by Bob Johnson on 2025-01-15",
]

inserted = 0
for text in sample_texts:
    encrypted = cipher.encrypt(text.encode()).decode()
    cur.execute(
        "INSERT INTO secure_data (encrypted_text) VALUES (%s)",
        (encrypted,)
    )
    inserted += 1

conn.commit()
logger.info(f"✅ Inserted {inserted} encrypted rows.")

cur.close()
conn.close()
logger.info("Done!")
