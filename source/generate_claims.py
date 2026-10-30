"""Synthetic claims generator for the Azure SQL source database.

All data is randomly generated. Nothing here is real claims data.

Usage (venv active, run from the project folder):
    python generate_claims.py init                  # create dbo.Claims from claims_ddl.sql
    python generate_claims.py seed --rows 5000      # initial load
    python generate_claims.py tick                  # one batch of inserts, updates, deletes
    python generate_claims.py tick --cycles 5 --interval 60

Needs a login that can write (your Azure admin login is fine for now), NOT the read-only
pipeline_user. Add these to .env:
    AZSQL_WRITER_USER=...
    AZSQL_WRITER_PASSWORD=...

Cost note: every command wakes the serverless database and keeps it awake while it runs,
which uses your free monthly compute allowance. Prefer short, one-off `tick` runs
(e.g. right before you test a pipeline run) over long loops.
"""
import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

load_dotenv()

# If fast_executemany ever raises a type error on a batch, set this to False.
USE_FAST_EXECUTEMANY = True

DDL_FILE = Path(__file__).with_name("claims_ddl.sql")
REQUIRED_ENV = ["AZSQL_SERVER", "AZSQL_DATABASE", "AZSQL_WRITER_USER", "AZSQL_WRITER_PASSWORD"]


# ---------------------------------------------------------------- connection
def connect(max_attempts=6, base_delay=10):
    """Connect with retries; a paused serverless DB needs time to wake."""
    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing values in .env: {', '.join(missing)}")

    password = os.environ["AZSQL_WRITER_PASSWORD"].replace("}", "}}")
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{os.environ['AZSQL_SERVER']},1433;"
        f"DATABASE={os.environ['AZSQL_DATABASE']};"
        f"UID={os.environ['AZSQL_WRITER_USER']};"
        f"PWD={{{password}}};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    for attempt in range(1, max_attempts + 1):
        try:
            return pyodbc.connect(conn_str, timeout=60)
        except pyodbc.Error as exc:
            if attempt == max_attempts:
                raise
            wait = base_delay * attempt
            print(f"Connect attempt {attempt} failed ({str(exc)[:100]}...). Retrying in {wait}s")
            time.sleep(wait)


# ---------------------------------------------------------------- fake data
FORM_TYPES = ["CMS1500", "UB04", "DENTAL"]
CLAIM_TYPES = ["MED", "DENT", "RX", "VIS"]
STATUSES = ["RECEIVED", "PENDED", "ADJUDICATED", "PAID", "DENIED", "CLOSED"]
PROCESSING = ["NEW", "AUTO", "MANUAL", "DONE"]
PAYMENT_CLASSES = ["STD", "CAP", "FFS"]
CATEGORIES = ["ROUTINE", "URGENT", "REVIEW"]
BILL_TYPES = ["111", "131", "831", None]

INSERT_COLS = [
    "AdjustmentVersion", "ClaimNumber", "FormType", "ClaimType",
    "ServiceDateFrom", "ServiceDateTo", "DateReceived", "DatePaid",
    "MemberId", "MemberCoverageId", "SubscriberId", "GroupId",
    "ProviderId", "OfficeId", "VendorId", "Status", "ExternalClaimNumber",
    "InputBatchID", "Priority", "AmtNegotiated", "AmtMaximumAllowable",
    "LastUpdatedBy", "LastUpdatedAt", "EntryDateTime", "ProcessingStatus",
    "PaymentClass", "ClaimCategoryCode", "TypeOfBill", "BilledCurrency",
]
INSERT_SQL = (
    f"INSERT INTO dbo.Claims ({', '.join(INSERT_COLS)}) "
    f"VALUES ({', '.join('?' for _ in INSERT_COLS)})"
)


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def make_claim():
    """One synthetic claim as a dict keyed by column name.

    DateReceived is backdated up to two years while LastUpdatedAt is 'now', so new rows
    look like late-arriving data: the received date is old but the row is new.
    """
    now = utc_now()
    received = (now - timedelta(days=random.randint(0, 730))).replace(hour=0, minute=0, second=0)
    service_from = received - timedelta(days=random.randint(1, 45))
    service_to = service_from + timedelta(days=random.randint(0, 5))
    status = random.choice(STATUSES)
    paid = status in ("PAID", "CLOSED")
    date_paid = min(received + timedelta(days=random.randint(5, 40)), now) if paid else None
    max_allowed = round(random.uniform(20, 5000), 2)
    member = random.randint(1, 20000)

    return {
        "AdjustmentVersion": 0,
        "ClaimNumber": uuid.uuid4().hex[:12].upper(),
        "FormType": random.choice(FORM_TYPES),
        "ClaimType": random.choice(CLAIM_TYPES),
        "ServiceDateFrom": service_from,
        "ServiceDateTo": service_to,
        "DateReceived": received,
        "DatePaid": date_paid,
        "MemberId": member,
        "MemberCoverageId": member * 10 + random.randint(1, 3),
        "SubscriberId": member // 2 + 1,
        "GroupId": random.randint(1, 200),
        "ProviderId": random.randint(1, 2000),
        "OfficeId": random.randint(1, 3000),
        "VendorId": random.randint(1, 50),
        "Status": status,
        "ExternalClaimNumber": "EXT-" + uuid.uuid4().hex[:10].upper(),
        "InputBatchID": random.randint(1, 500),
        "Priority": round(random.uniform(0, 10), 1),
        "AmtNegotiated": round(max_allowed * random.uniform(0.5, 1.0), 2),
        "AmtMaximumAllowable": max_allowed,
        "LastUpdatedBy": random.randint(1, 25),
        "LastUpdatedAt": now,
        "EntryDateTime": now,
        "ProcessingStatus": random.choice(PROCESSING),
        "PaymentClass": random.choice(PAYMENT_CLASSES),
        "ClaimCategoryCode": random.choice(CATEGORIES),
        "TypeOfBill": random.choice(BILL_TYPES),
        "BilledCurrency": "USD",
    }


def insert_claims(cur, n, batch_size=1000):
    cur.fast_executemany = USE_FAST_EXECUTEMANY
    remaining = n
    while remaining > 0:
        size = min(batch_size, remaining)
        rows = []
        for _ in range(size):
            claim = make_claim()
            rows.append(tuple(claim[c] for c in INSERT_COLS))
        cur.executemany(INSERT_SQL, rows)
        remaining -= size


# Random rows are picked in SQL, but the NEW values are chosen in Python, one set per claim.
# Choosing them in SQL with CHOOSE(... NEWID() ...) is a bug: SQL Server re-evaluates NEWID()
# for every branch of the CHOOSE, so the index can miss all branches and the result is NULL.
# Every UPDATE gives the row a new LastActiveRowVersionNumber automatically.
UPDATE_STATUSES = ["PENDED", "ADJUDICATED", "PAID", "DENIED"]
UPDATE_PROCESSING = ["AUTO", "MANUAL", "DONE"]

PICK_SQL = "SELECT TOP (?) ClaimID, AmtMaximumAllowable FROM dbo.Claims ORDER BY NEWID()"
UPDATE_ONE_SQL = """
UPDATE dbo.Claims SET
    Status            = ?,
    ProcessingStatus  = ?,
    AmtNegotiated     = ?,
    AdjustmentVersion = ISNULL(AdjustmentVersion, 0) + 1,
    LastUpdatedBy     = ?,
    LastUpdatedAt     = SYSUTCDATETIME()
WHERE ClaimID = ?
"""


def update_random_claims(cur, n):
    """Change status, processing status and negotiated amount for n random claims."""
    cur.execute(PICK_SQL, n)
    picked = cur.fetchall()
    params = []
    for claim_id, max_allowed in picked:
        factor = Decimal(str(round(random.uniform(0.5, 0.99), 2)))
        negotiated = (max_allowed * factor).quantize(Decimal("0.01"))
        params.append((
            random.choice(UPDATE_STATUSES),
            random.choice(UPDATE_PROCESSING),
            negotiated,
            random.randint(1, 25),
            claim_id,
        ))
    if params:
        cur.fast_executemany = USE_FAST_EXECUTEMANY
        cur.executemany(UPDATE_ONE_SQL, params)
    return len(params)

DELETE_SQL = """
WITH picked AS (SELECT TOP (?) ClaimID FROM dbo.Claims ORDER BY NEWID())
DELETE FROM picked
"""


def run_tick(cur, inserts, updates, deletes):
    if inserts:
        insert_claims(cur, inserts)
    n_updated = n_deleted = 0
    if updates:
        n_updated = update_random_claims(cur, updates)
    if deletes:
        cur.execute(DELETE_SQL, deletes)
        n_deleted = cur.rowcount
    return n_updated, n_deleted


# ---------------------------------------------------------------- commands
def cmd_init(_args):
    ddl = DDL_FILE.read_text(encoding="utf-8")
    with connect() as conn:
        conn.cursor().execute(ddl)
        conn.commit()
        print("dbo.Claims is ready (created if it did not exist).")


def cmd_seed(args):
    with connect() as conn:
        cur = conn.cursor()
        insert_claims(cur, args.rows)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM dbo.Claims")
        print(f"Inserted {args.rows} claims. Table now has {cur.fetchone()[0]} rows.")


def cmd_tick(args):
    with connect() as conn:
        cur = conn.cursor()
        for i in range(1, args.cycles + 1):
            updated, deleted = run_tick(cur, args.inserts, args.updates, args.deletes)
            conn.commit()
            print(f"[cycle {i}/{args.cycles}] inserted {args.inserts}, updated {updated}, deleted {deleted}")
            if i < args.cycles:
                time.sleep(args.interval)
        cur.execute("SELECT COUNT(*) FROM dbo.Claims")
        print(f"Table now has {cur.fetchone()[0]} rows.")


def main():
    parser = argparse.ArgumentParser(description="Synthetic claims generator")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create dbo.Claims").set_defaults(func=cmd_init)

    p_seed = sub.add_parser("seed", help="insert an initial batch of claims")
    p_seed.add_argument("--rows", type=int, default=5000)
    p_seed.set_defaults(func=cmd_seed)

    p_tick = sub.add_parser("tick", help="insert new claims, update some, delete a few")
    p_tick.add_argument("--inserts", type=int, default=50)
    p_tick.add_argument("--updates", type=int, default=100)
    p_tick.add_argument("--deletes", type=int, default=2)
    p_tick.add_argument("--cycles", type=int, default=1)
    p_tick.add_argument("--interval", type=int, default=30, help="seconds between cycles")
    p_tick.set_defaults(func=cmd_tick)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
