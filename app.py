import psycopg2
import psycopg2.pool
import os
import sys
import time
import json
import logging
import valkey

from fastapi import FastAPI, Query, Request, HTTPException
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from pydantic import BaseModel
from contextlib import contextmanager
from transformers import pipeline

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor

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
DB_HOST = os.getenv("DB_HOST", "postgres")
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
# Rate Limiting (Valkey)
# --------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

redis_client = valkey.Valkey(host=REDIS_HOST, port=6379, decode_responses=True)
RATE_LIMIT = 5
WINDOW = 60


def check_rate_limit(request: Request):

    forwarded = request.headers.get("x-forwarded-for")

    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        ip = request.client.host

    rate_key = f"rate_limit:{ip}"
    block_key = f"blocked:{ip}"

    # If user already blocked
    if redis_client.exists(block_key):

        ttl = redis_client.ttl(block_key)

        if ttl < 0:
            ttl = WINDOW

        raise HTTPException(
            status_code=429,
            detail=f"You are temporarily blocked. Try again in {ttl} seconds."
        )

    count = redis_client.incr(rate_key)

    if count == 1:
        redis_client.expire(rate_key, WINDOW)

    if count > RATE_LIMIT:

        # only set block if it doesn't exist
        if not redis_client.exists(block_key):
            redis_client.setex(block_key, WINDOW, "blocked")

        ttl = redis_client.ttl(block_key)

        raise HTTPException(
            status_code=429,
            detail=f"Too many requests. You are blocked for {ttl} seconds."
        )


# --------------------------------
# Database Setup
# --------------------------------

MAX_RETRIES = 10
RETRY_DELAY = 3


def ensure_database_exists():

    logger.info(f"Checking if database '{DB_NAME}' exists")

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database="postgres",
                user=DB_USER,
                password=DB_PASSWORD
            )

            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s",
                (DB_NAME,)
            )

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
                logger.warning(f"Waiting for PostgreSQL... {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
            else:
                logger.error("PostgreSQL connection failed")
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


# --------------------------------
# Connection Pool
# --------------------------------

pool = None

for attempt in range(1, MAX_RETRIES + 1):

    try:

        pool = psycopg2.pool.ThreadedConnectionPool(
            1, 10,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        logger.info("Connected to PostgreSQL")
        break

    except psycopg2.OperationalError:

        if attempt < MAX_RETRIES:
            logger.warning(f"Retrying PostgreSQL connection {attempt}/{MAX_RETRIES}")
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


# create table once
with get_db() as conn:
    ensure_table_exists(conn)


# --------------------------------
# Sentiment Model
# --------------------------------

logger.info("Loading sentiment model")

classifier = pipeline(
    "sentiment-analysis",
    model="nlptown/bert-base-multilingual-uncased-sentiment"
)

logger.info("Sentiment model ready")


def get_emoji(stars, confidence):

    if confidence < 50:
        return "❓"

    emoji_map = {
        5: "😍",
        4: "😄",
        3: "😶",
        2: "😕",
        1: "😡"
    }

    return emoji_map.get(stars, "😶")

# --------------------------------
# OpenTelemetry Tracing
# --------------------------------

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "encrypted-review-api")

resource = Resource(attributes={
    "service.name": SERVICE_NAME
})

# create tracer provider
provider = TracerProvider(resource=resource)

# set tracer provider
trace.set_tracer_provider(provider)

# OTLP exporter (SigNoz collector)
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

otlp_exporter = OTLPSpanExporter(
    endpoint=OTEL_ENDPOINT,
    insecure=True
)
# span processor
span_processor = BatchSpanProcessor(otlp_exporter)

# attach processor to provider
provider.add_span_processor(span_processor)



# --------------------------------
# FastAPI
# --------------------------------
DOCS_ENABLED = os.getenv("DOCS", "true").lower() == "true"

app = FastAPI(
    title="Encrypted Review API",
    docs_url="/docs" if DOCS_ENABLED else None,
    redoc_url="/redoc" if DOCS_ENABLED else None,
    openapi_url="/openapi.json" if DOCS_ENABLED else None
)

FastAPIInstrumentor.instrument_app(app)
Psycopg2Instrumentor().instrument()


@app.get("/")
def home():
    return {"message": "API running"}

class ReviewInput(BaseModel):
    email: str
    review: str


# --------------------------------
# Add Review
# --------------------------------

@app.post("/review")
def add_review(data: ReviewInput, request: Request):

    check_rate_limit(request)

    result = classifier(data.review)[0]

    stars = int(result["label"][0])
    confidence = round(result["score"] * 100, 2)

    emoji = get_emoji(stars, confidence)

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

    return {
        "status": "saved",
        "id": row[0],
        "stars": stars,
        "confidence": confidence,
        "emoji": emoji,
        "created_at": str(row[1])
    }


# --------------------------------
# Get Reviews
# --------------------------------

@app.get("/reviews")
def get_reviews(request: Request):

    check_rate_limit(request)

    with get_db() as conn:

        cur = conn.cursor()

        cur.execute(
            "SELECT id, encrypted_text, created_at FROM secure_data ORDER BY created_at DESC"
        )

        rows = cur.fetchall()
        cur.close()

    results = []

    for row in rows:

        try:

            decrypted = json.loads(
                cipher.decrypt(row[1].encode()).decode()
            )

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
            logger.warning(f"Decryption failed for id={row[0]}")

    return {"total": len(results), "reviews": results}


# --------------------------------
# Search Reviews
# --------------------------------

@app.get("/search")
def search_reviews(request: Request, q: str = Query(..., min_length=1)):

    check_rate_limit(request)

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
            logger.warning(f"Decryption failed for id={row[0]}")

    return {
        "query": q,
        "total_scanned": len(rows),
        "matches": len(results),
        "results": results
    }

@app.get("/analytics")
def analytics():

    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("SELECT encrypted_text FROM secure_data")
        rows = cur.fetchall()

        cur.close()

    stars = {1:0,2:0,3:0,4:0,5:0}

    for row in rows:
        try:
            decrypted = json.loads(cipher.decrypt(row[0].encode()).decode())
            star = decrypted.get("stars")

            if star in stars:
                stars[star] += 1

        except:
            pass

    return {
        "total_reviews": sum(stars.values()),
        "stars_distribution": stars
    }
