import pandas as pd


MERCHANT_MAP = {
    "amzn": "Amazon",
    "amazon.com": "Amazon",
    "amazon": "Amazon",
    "starbucks coffee": "Starbucks",
    "sbux": "Starbucks",
    "walmart inc": "Walmart",
    "wal-mart": "Walmart",
    "uber trip": "Uber",
    "uber": "Uber",
}

CATEGORY_MAP = {
    "food & drink": "Food",
    "dining": "Food",
    "restaurants": "Food",
    "groceries": "Groceries",
    "shopping": "Shopping",
    "travel": "Travel",
    "transportation": "Transport",
    "transport": "Transport",
    "entertainment": "Entertainment",
}

PAYMENT_METHOD_MAP = {
    "credit card": "Card",
    "debit card": "Card",
    "card": "Card",
    "ach transfer": "ACH",
    "bank transfer": "ACH",
    "cash": "Cash",
    "paypal": "Digital Wallet",
    "apple pay": "Digital Wallet",
    "google pay": "Digital Wallet",
}

STATUS_MAP = {
    "complete": "Completed",
    "completed": "Completed",
    "posted": "Completed",
    "success": "Completed",
    "pending": "Pending",
    "failed": "Failed",
    "declined": "Failed",
    "reversed": "Reversed",
}


VALID_TRANSACTION_TYPES = {"debit", "credit"}


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _standardize_with_map(raw_value: object, mapping: dict, default_title: bool = True) -> str:
    value = _normalize_text(raw_value)
    if not value:
        return "Unknown"

    lowered = value.lower()
    if lowered in mapping:
        return mapping[lowered]

    return value.title() if default_title else value


def transform_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean, normalize, validate, and engineer features.
    Returns:
        clean_df: rows that passed validation
        invalid_df: rows that failed validation with error_reason
    """
    working_df = df.copy()

    # Standardize whitespace on object columns
    object_columns = working_df.select_dtypes(include="object").columns
    for col in object_columns:
        working_df[col] = working_df[col].astype(str).str.strip()

    # Replace stringified nan/none values back to proper missing values
    working_df.replace(
        {
            "nan": pd.NA,
            "None": pd.NA,
            "": pd.NA,
        },
        inplace=True,
    )

    # Remove duplicate transaction IDs, keep first occurrence
    working_df.drop_duplicates(subset=["transaction_id"], keep="first", inplace=True)

    # Normalize core fields
    working_df["merchant"] = working_df["merchant"].apply(
        lambda x: _standardize_with_map(x, MERCHANT_MAP)
    )
    working_df["category"] = working_df["category"].apply(
        lambda x: _standardize_with_map(x, CATEGORY_MAP)
    )
    working_df["payment_method"] = working_df["payment_method"].apply(
        lambda x: _standardize_with_map(x, PAYMENT_METHOD_MAP, default_title=False)
    )
    working_df["status"] = working_df["status"].apply(
        lambda x: _standardize_with_map(x, STATUS_MAP)
    )

    # Normalize transaction_type
    working_df["transaction_type"] = (
        working_df["transaction_type"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # Parse dates and amounts
    working_df["parsed_date"] = pd.to_datetime(
        working_df["date"], errors="coerce", infer_datetime_format=True
    )
    working_df["parsed_amount"] = pd.to_numeric(
        working_df["amount"], errors="coerce"
    )

    # Build validation errors
    error_reasons = []

    for _, row in working_df.iterrows():
        row_errors = []

        if pd.isna(row["transaction_id"]):
            row_errors.append("missing_transaction_id")

        if pd.isna(row["parsed_date"]):
            row_errors.append("invalid_date")

        if pd.isna(row["account_id"]):
            row_errors.append("missing_account_id")

        if pd.isna(row["parsed_amount"]):
            row_errors.append("invalid_amount")

        if row["transaction_type"] not in VALID_TRANSACTION_TYPES:
            row_errors.append("invalid_transaction_type")

        error_reasons.append(", ".join(row_errors) if row_errors else None)

    working_df["error_reason"] = error_reasons

    invalid_df = working_df[working_df["error_reason"].notna()].copy()
    clean_df = working_df[working_df["error_reason"].isna()].copy()

    # Finalize clean fields
    clean_df["date"] = clean_df["parsed_date"]
    clean_df["amount"] = clean_df["parsed_amount"]

    # Feature engineering
    clean_df["transaction_month"] = clean_df["date"].dt.month
    clean_df["transaction_year"] = clean_df["date"].dt.year
    clean_df["debit_credit_flag"] = clean_df["transaction_type"].str.upper()
    clean_df["absolute_amount"] = clean_df["amount"].abs()
    clean_df["high_value_transaction"] = clean_df["absolute_amount"] >= 100
    clean_df["weekend_transaction"] = clean_df["date"].dt.dayofweek >= 5

    # Merchant frequency per merchant in current dataset
    merchant_counts = clean_df["merchant"].value_counts(dropna=False).to_dict()
    clean_df["merchant_frequency"] = clean_df["merchant"].map(merchant_counts)

    # Keep columns in a clean order
    clean_columns = [
        "transaction_id",
        "date",
        "account_id",
        "merchant",
        "category",
        "amount",
        "transaction_type",
        "location",
        "payment_method",
        "status",
        "transaction_month",
        "transaction_year",
        "debit_credit_flag",
        "absolute_amount",
        "high_value_transaction",
        "weekend_transaction",
        "merchant_frequency",
    ]

    invalid_columns = list(df.columns) + ["parsed_date", "parsed_amount", "error_reason"]

    clean_df = clean_df[clean_columns].copy()
    invalid_df = invalid_df[invalid_columns].copy()

    return clean_df, invalid_df