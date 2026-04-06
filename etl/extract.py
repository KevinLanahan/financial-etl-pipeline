import pandas as pd

def extract_data(file_path):
    df = pd.read_csv(file_path)

    required_columns = [
        "transaction_id", "date", "account_id", "merchant",
        "category", "amount", "transaction_type"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    return df