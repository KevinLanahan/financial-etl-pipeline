import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd

from config import DB_CONFIG


CREATE_CLEAN_TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS clean_transactions (
    transaction_id BIGINT PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    account_id VARCHAR(100) NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    location VARCHAR(100),
    payment_method VARCHAR(100),
    status VARCHAR(50),
    transaction_month INT NOT NULL,
    transaction_year INT NOT NULL,
    debit_credit_flag VARCHAR(20) NOT NULL,
    absolute_amount NUMERIC(12, 2) NOT NULL,
    high_value_transaction BOOLEAN NOT NULL,
    weekend_transaction BOOLEAN NOT NULL,
    merchant_frequency INT NOT NULL
);
"""

CREATE_INVALID_TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS invalid_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    date VARCHAR(100),
    account_id VARCHAR(100),
    merchant VARCHAR(255),
    category VARCHAR(100),
    amount VARCHAR(100),
    transaction_type VARCHAR(50),
    location VARCHAR(100),
    payment_method VARCHAR(100),
    status VARCHAR(50),
    parsed_date TIMESTAMP NULL,
    parsed_amount NUMERIC(12, 2) NULL,
    error_reason TEXT NOT NULL
);
"""


def get_connection():
    return psycopg2.connect(**DB_CONFIG.connection_dict)


def create_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_CLEAN_TRANSACTIONS_TABLE)
            cursor.execute(CREATE_INVALID_TRANSACTIONS_TABLE)
        conn.commit()


def truncate_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE clean_transactions RESTART IDENTITY;")
            cursor.execute("TRUNCATE TABLE invalid_transactions RESTART IDENTITY;")
        conn.commit()


def load_clean_transactions(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    rows = [
        (
            int(row["transaction_id"]),
            row["date"].to_pydatetime() if hasattr(row["date"], "to_pydatetime") else row["date"],
            row["account_id"],
            row["merchant"],
            row["category"],
            float(row["amount"]),
            row["transaction_type"],
            row["location"],
            row["payment_method"],
            row["status"],
            int(row["transaction_month"]),
            int(row["transaction_year"]),
            row["debit_credit_flag"],
            float(row["absolute_amount"]),
            bool(row["high_value_transaction"]),
            bool(row["weekend_transaction"]),
            int(row["merchant_frequency"]),
        )
        for _, row in df.iterrows()
    ]

    insert_sql = """
    INSERT INTO clean_transactions (
        transaction_id,
        date,
        account_id,
        merchant,
        category,
        amount,
        transaction_type,
        location,
        payment_method,
        status,
        transaction_month,
        transaction_year,
        debit_credit_flag,
        absolute_amount,
        high_value_transaction,
        weekend_transaction,
        merchant_frequency
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_sql, rows, page_size=500)
        conn.commit()

    return len(rows)


def load_invalid_transactions(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    rows = [
        (
            None if pd.isna(row.get("transaction_id")) else str(row.get("transaction_id")),
            None if pd.isna(row.get("date")) else str(row.get("date")),
            None if pd.isna(row.get("account_id")) else str(row.get("account_id")),
            None if pd.isna(row.get("merchant")) else str(row.get("merchant")),
            None if pd.isna(row.get("category")) else str(row.get("category")),
            None if pd.isna(row.get("amount")) else str(row.get("amount")),
            None if pd.isna(row.get("transaction_type")) else str(row.get("transaction_type")),
            None if pd.isna(row.get("location")) else str(row.get("location")),
            None if pd.isna(row.get("payment_method")) else str(row.get("payment_method")),
            None if pd.isna(row.get("status")) else str(row.get("status")),
            None if pd.isna(row.get("parsed_date")) else row.get("parsed_date").to_pydatetime(),
            None if pd.isna(row.get("parsed_amount")) else float(row.get("parsed_amount")),
            row.get("error_reason"),
        )
        for _, row in df.iterrows()
    ]

    insert_sql = """
    INSERT INTO invalid_transactions (
        transaction_id,
        date,
        account_id,
        merchant,
        category,
        amount,
        transaction_type,
        location,
        payment_method,
        status,
        parsed_date,
        parsed_amount,
        error_reason
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_sql, rows, page_size=500)
        conn.commit()

    return len(rows)