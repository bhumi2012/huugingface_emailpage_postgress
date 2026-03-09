import psycopg2
import psycopg2.pool
import os
import sys
import time
import json
import logging

from cryptography.fernet import Fernet
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from pydantic import BaseModel
from contextlib import contextmanager
from transformers import pipeline

# --------------------------------
# Logging
# --------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("encrypted_search")

# --------------------------------
# Environment Variables
# --------------------------------

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("APP_DB", os.getenv("POSTGRES_DB", "encrypted_db"))
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not DB_USER or not DB_PASSWORD or not DB_NAME or not ENCRYPTION_KEY:
    logger.error("Missing required environment variables")
    sys.exit(1)

logger.info("Environment variables loaded")

# --------------------------------
# Encryption
# --------------------------------

cipher = Fernet(ENCRYPTION_KEY.encode())
logger.info("Fernet encryption initialized")

# --------------------------------
# Database Setup
# --------------------------------

MAX_RETRIES = 10
RETRY_DELAY = 3


def ensure_database_exists():
    logger.info(f"Checking if database '{DB_NAME}' exists...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, database="postgres",
                user=DB_USER, password=DB_PASSWORD
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{DB_NAME}"')
                logger.info(f"Database '{DB_NAME}' created")
            else:
                logger.info(f"Database '{DB_NAME}' already exists")
            cur.close()
            conn.close()
            return
        except psycopg2.OperationalError:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} — waiting for PostgreSQL...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error("Cannot connect to PostgreSQL")
                sys.exit(1)


def ensure_table_exists(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS secure_data (
            id SERIAL PRIMARY KEY,
            encrypted_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    logger.info("Table 'secure_data' ready")


ensure_database_exists()

pool = None
for attempt in range(1, MAX_RETRIES + 1):
    try:
        pool = psycopg2.pool.ThreadedConnectionPool(
            1, 10,
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        logger.info(f"Connected to PostgreSQL ({DB_HOST}:{DB_PORT}/{DB_NAME})")
        break
    except psycopg2.OperationalError:
        if attempt < MAX_RETRIES:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} — waiting for PostgreSQL...")
            time.sleep(RETRY_DELAY)
        else:
            logger.error("Database connection failed")
            sys.exit(1)


@contextmanager
def get_db():
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


# Create table on startup
with get_db() as conn:
    ensure_table_exists(conn)

# --------------------------------
# Sentiment Analysis Model
# --------------------------------

logger.info("Loading sentiment analysis model...")
classifier = pipeline(
    "sentiment-analysis",
    model="nlptown/bert-base-multilingual-uncased-sentiment"
)
logger.info("Sentiment model loaded")


def get_emoji(stars, confidence):
    if confidence < 50:
        return "❓"
    emoji_map = {5: "😍", 4: "😄", 3: "😶", 2: "😕", 1: "😡"}
    return emoji_map.get(stars, "😶")


# --------------------------------
# FastAPI App
# --------------------------------

app = FastAPI(title="Encrypted Review Search API")


class ReviewInput(BaseModel):
    email: str
    review: str


@app.post("/review")
def add_review(data: ReviewInput):
    """Encrypt and store a review with sentiment analysis in PostgreSQL."""
    # Run sentiment analysis
    result = classifier(data.review)[0]
    stars = int(result["label"][0])
    confidence = round(result["score"] * 100, 2)
    emoji = get_emoji(stars, confidence)

    logger.info(f"Review: '{data.review}' → {stars} stars ({confidence}%) → {emoji}")

    review_data = json.dumps({
        "email": data.email,
        "review": data.review,
        "stars": stars,
        "confidence": confidence,
        "emoji": emoji
    })
    encrypted = cipher.encrypt(review_data.encode()).decode()

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO secure_data (encrypted_text) VALUES (%s) RETURNING id, created_at",
            (encrypted,)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()

    logger.info(f"Stored review id={row[0]}")
    return {
        "status": "saved",
        "id": row[0],
        "stars": stars,
        "confidence": confidence,
        "emoji": emoji,
        "created_at": str(row[1])
    }


@app.get("/reviews")
def get_reviews():
    """Decrypt and return all stored reviews."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, encrypted_text, created_at FROM secure_data ORDER BY created_at DESC")
        rows = cur.fetchall()
        cur.close()

    results = []
    for row in rows:
        try:
            decrypted = json.loads(cipher.decrypt(row[1].encode()).decode())
            results.append({
                "id": row[0],
                "email": decrypted.get("email", ""),
                "review": decrypted.get("review", ""),
                "stars": decrypted.get("stars", ""),
                "confidence": decrypted.get("confidence", ""),
                "emoji": decrypted.get("emoji", ""),
                "created_at": str(row[2])
            })
        except Exception:
            logger.warning(f"Could not decrypt row id={row[0]}, skipping")

    return {"total": len(results), "reviews": results}


@app.get("/search")
def search_reviews(q: str = Query(..., min_length=1, description="Search term")):
    """Decrypt all reviews and return those matching the search term (partial, case-insensitive)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, encrypted_text, created_at FROM secure_data")
        rows = cur.fetchall()
        cur.close()

    results = []
    search_lower = q.lower()

    for row in rows:
        try:
            decrypted_str = cipher.decrypt(row[1].encode()).decode()
            if search_lower in decrypted_str.lower():
                decrypted = json.loads(decrypted_str)
                results.append({
                    "id": row[0],
                    "email": decrypted.get("email", ""),
                    "review": decrypted.get("review", ""),
                    "stars": decrypted.get("stars", ""),
                    "confidence": decrypted.get("confidence", ""),
                    "emoji": decrypted.get("emoji", ""),
                    "created_at": str(row[2])
                })
        except Exception:
            logger.warning(f"Could not decrypt row id={row[0]}, skipping")

    logger.info(f"Search '{q}' — scanned {len(rows)} rows, found {len(results)} match(es)")
    return {"query": q, "total_scanned": len(rows), "matches": len(results), "results": results}
