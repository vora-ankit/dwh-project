import os
import time

import pyodbc
from dotenv import load_dotenv

load_dotenv()

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{os.environ['AZSQL_SERVER']},1433;"
    f"DATABASE={os.environ['AZSQL_DATABASE']};"
    f"UID={os.environ['AZSQL_USER']};"
    f"PWD={{{os.environ['AZSQL_PASSWORD']}}};"
    "Encrypt=yes;TrustServerCertificate=no;"
)


def get_connection(max_attempts=5, base_delay=10):
    """Connect with retries, since a paused serverless DB needs time to wake."""
    for attempt in range(1, max_attempts + 1):
        try:
            return pyodbc.connect(CONN_STR, timeout=60)
        except pyodbc.Error as exc:
            if attempt == max_attempts:
                raise
            wait = base_delay * attempt
            print(f"Attempt {attempt} failed: {exc}\nRetrying in {wait}s...")
            time.sleep(wait)


if __name__ == "__main__":
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        print(cursor.fetchone()[0])

        cursor.execute("SELECT TOP 5 name FROM sys.tables ORDER BY name")
        for row in cursor.fetchall():
            print(row.name)