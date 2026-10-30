"""Synthetic providers and members for the Azure SQL source database.

All names, IDs and dates are randomly generated. Nothing here is real data.

Usage (venv active, project folder, same .env as generate_claims.py):
    python generate_dims.py init                 # create dbo.Providers and dbo.Members
    python generate_dims.py seed                 # 2,000 providers and 20,000 members (run ONCE)
    python generate_dims.py tick                 # new rows plus some updates

Why 2,000 and 20,000: generate_claims.py picks ProviderId from 1..2000 and MemberId from
1..20000. Seeding a fresh table gives exactly those IDs (the keys are IDENTITY columns
starting at 1), so every claim points at a real provider and member. `seed` refuses to run
on a table that already has rows, so the IDs cannot drift.

tick updates change attributes that matter for slowly-changing-dimension practice later:
a provider's network status and location, a member's plan type and address.
"""
import argparse
import random
from datetime import date, timedelta
from pathlib import Path

from generate_claims import USE_FAST_EXECUTEMANY, connect

DDL_FILE = Path(__file__).with_name("dims_ddl.sql")

FIRST_NAMES = ["Alex", "Blake", "Casey", "Dana", "Eli", "Fran", "Gray", "Harper", "Indy", "Jules",
               "Kai", "Lane", "Morgan", "Noel", "Onyx", "Parker", "Quinn", "Reese", "Sage", "Tatum"]
LAST_NAMES = ["Ashford", "Brook", "Calder", "Dalton", "Ellery", "Fairfax", "Garland", "Hollis",
              "Ingram", "Jarrett", "Kendall", "Lowell", "Marlow", "Newell", "Oakley", "Prescott",
              "Quill", "Rowan", "Sutton", "Thorne"]
FACILITY_WORDS = ["Medical Center", "Health Clinic", "Imaging Center", "Surgical Associates",
                  "Family Practice", "Urgent Care", "Diagnostic Lab"]
SPECIALTIES = ["Family Medicine", "Internal Medicine", "Pediatrics", "Cardiology", "Orthopedics",
               "Dermatology", "Radiology", "Psychiatry", "Oncology", "Emergency Medicine"]
LOCATIONS = [("Albany", "NY"), ("Buffalo", "NY"), ("Newark", "NJ"), ("Trenton", "NJ"),
             ("Hartford", "CT"), ("Stamford", "CT"), ("Boston", "MA"), ("Worcester", "MA"),
             ("Philadelphia", "PA"), ("Pittsburgh", "PA"), ("Baltimore", "MD"), ("Richmond", "VA")]
PLAN_TYPES = ["HMO", "PPO", "EPO", "HDHP"]

PROVIDER_COLS = ["NPI", "ProviderName", "ProviderType", "Specialty", "City", "State", "PostalCode",
                 "NetworkStatus", "ContractStartDate", "IsActive"]
MEMBER_COLS = ["FirstName", "LastName", "DateOfBirth", "Gender", "State", "PostalCode", "PlanType",
               "GroupID", "EffectiveDate", "TerminationDate"]


def insert_sql(table, cols):
    return f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"


PROVIDER_INSERT = insert_sql("dbo.Providers", PROVIDER_COLS)
MEMBER_INSERT = insert_sql("dbo.Members", MEMBER_COLS)


def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))


def make_provider():
    """One synthetic provider as a dict keyed by column name."""
    if random.random() < 0.7:
        name = f"Dr. {random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        ptype, specialty = "INDIVIDUAL", random.choice(SPECIALTIES)
    else:
        name = f"{random.choice(LAST_NAMES)} {random.choice(FACILITY_WORDS)}"
        ptype, specialty = "FACILITY", None
    city, state = random.choice(LOCATIONS)
    return {
        "NPI": str(random.randint(10**9, 10**10 - 1)),
        "ProviderName": name,
        "ProviderType": ptype,
        "Specialty": specialty,
        "City": city,
        "State": state,
        "PostalCode": f"{random.randint(10000, 99999)}",
        "NetworkStatus": "IN" if random.random() < 0.8 else "OUT",
        "ContractStartDate": random_date(date(2010, 1, 1), date(2025, 12, 31)),
        "IsActive": True,
    }


def make_member():
    """One synthetic member as a dict keyed by column name."""
    city, state = random.choice(LOCATIONS)
    effective = random_date(date(2018, 1, 1), date(2026, 6, 30))
    terminated = effective + timedelta(days=random.randint(90, 900)) if random.random() < 0.1 else None
    return {
        "FirstName": random.choice(FIRST_NAMES),
        "LastName": random.choice(LAST_NAMES),
        "DateOfBirth": random_date(date(1940, 1, 1), date(2022, 12, 31)),
        "Gender": random.choice(["M", "F"]),
        "State": state,
        "PostalCode": f"{random.randint(10000, 99999)}",
        "PlanType": random.choice(PLAN_TYPES),
        "GroupID": random.randint(1, 200),
        "EffectiveDate": effective,
        "TerminationDate": terminated,
    }


def insert_rows(cur, sql, cols, maker, n, batch_size=1000):
    cur.fast_executemany = USE_FAST_EXECUTEMANY
    remaining = n
    while remaining > 0:
        size = min(batch_size, remaining)
        rows = []
        for _ in range(size):
            row = maker()
            rows.append(tuple(row[c] for c in cols))
        cur.executemany(sql, rows)
        remaining -= size


UPDATE_PROVIDERS = """
WITH picked AS (SELECT TOP (?) * FROM dbo.Providers ORDER BY NEWID())
UPDATE picked SET
    NetworkStatus = CASE NetworkStatus WHEN 'IN' THEN 'OUT' ELSE 'IN' END,
    LastUpdatedAt = SYSUTCDATETIME()
"""

# Members change plan type and postal code, the attributes a slowly changing dimension tracks.
# The new values are chosen in Python, one per member. Doing it in SQL with
# CHOOSE(... NEWID() ...) is a bug: SQL Server re-evaluates NEWID() for every branch of the
# CHOOSE, so the index can miss all branches and the result is NULL.
PICK_MEMBERS = "SELECT TOP (?) MemberID FROM dbo.Members ORDER BY NEWID()"
UPDATE_MEMBER = """
UPDATE dbo.Members
SET PlanType = ?, PostalCode = ?, LastUpdatedAt = SYSUTCDATETIME()
WHERE MemberID = ?
"""


def update_random_members(cur, n):
    """Change plan type and postal code for n random members. Returns how many were updated."""
    cur.execute(PICK_MEMBERS, n)
    ids = [row[0] for row in cur.fetchall()]
    params = [
        (random.choice(PLAN_TYPES), f"{random.randint(10000, 99999)}", member_id)
        for member_id in ids
    ]
    if params:
        cur.fast_executemany = USE_FAST_EXECUTEMANY
        cur.executemany(UPDATE_MEMBER, params)
    return len(params)


def cmd_init(_args):
    with connect() as conn:
        conn.cursor().execute(DDL_FILE.read_text(encoding="utf-8"))
        conn.commit()
        print("dbo.Providers and dbo.Members are ready (created if they did not exist).")


def cmd_seed(args):
    with connect() as conn:
        cur = conn.cursor()
        for table, id_col, sql, cols, maker, n in [
            ("dbo.Providers", "ProviderID", PROVIDER_INSERT, PROVIDER_COLS, make_provider, args.providers),
            ("dbo.Members", "MemberID", MEMBER_INSERT, MEMBER_COLS, make_member, args.members),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            existing = cur.fetchone()[0]
            if existing:
                raise SystemExit(f"{table} already has {existing} rows; seed only runs on an empty table.")
            insert_rows(cur, sql, cols, maker, n)
            conn.commit()
            cur.execute(f"SELECT COUNT(*), MAX({id_col}) FROM {table}")
            count, max_id = cur.fetchone()
            print(f"{table}: {count} rows, highest ID {max_id}")


def cmd_tick(args):
    with connect() as conn:
        cur = conn.cursor()
        insert_rows(cur, PROVIDER_INSERT, PROVIDER_COLS, make_provider, args.new_providers)
        insert_rows(cur, MEMBER_INSERT, MEMBER_COLS, make_member, args.new_members)
        cur.execute(UPDATE_PROVIDERS, args.update_providers)
        updated_providers = cur.rowcount
        updated_members = update_random_members(cur, args.update_members)
        conn.commit()
        print(f"providers: +{args.new_providers} new, {updated_providers} updated")
        print(f"members:   +{args.new_members} new, {updated_members} updated")


def main():
    parser = argparse.ArgumentParser(description="Synthetic providers and members")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create the tables").set_defaults(func=cmd_init)

    p_seed = sub.add_parser("seed", help="initial load (run once)")
    p_seed.add_argument("--providers", type=int, default=2000)
    p_seed.add_argument("--members", type=int, default=20000)
    p_seed.set_defaults(func=cmd_seed)

    p_tick = sub.add_parser("tick", help="add a few rows and update some")
    p_tick.add_argument("--new-providers", type=int, default=1)
    p_tick.add_argument("--new-members", type=int, default=20)
    p_tick.add_argument("--update-providers", type=int, default=5)
    p_tick.add_argument("--update-members", type=int, default=50)
    p_tick.set_defaults(func=cmd_tick)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
