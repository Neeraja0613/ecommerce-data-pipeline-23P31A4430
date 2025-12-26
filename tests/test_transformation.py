import psycopg2
import os
import pytest

@pytest.fixture
def db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    yield conn
    conn.rollback()
    conn.close()

def test_production_tables_populated(db_connection):
    tables = ["production.customers", "production.products", "production.transactions", "production.transaction_items"]
    cur = db_connection.cursor()
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        assert count > 0

def test_no_orphan_records(db_connection):
    cur = db_connection.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM production.transactions t
        LEFT JOIN production.customers c ON t.customer_id=c.customer_id
        WHERE c.customer_id IS NULL
    """)
    assert cur.fetchone()[0] == 0

def test_data_cleansing(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT email FROM production.customers")
    emails = [r[0] for r in cur.fetchall()]
    assert all(e == e.lower().strip() for e in emails)

def test_business_rules(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT price, cost, profit_margin FROM production.products")
    for price, cost, margin in cur.fetchall():
        assert price > 0
        expected_margin = round((price - cost) / price, 2)
        assert round(margin if margin is not None else expected_margin, 2) == expected_margin
