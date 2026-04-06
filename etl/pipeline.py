from pathlib import Path

from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, INVALID_DATA_PATH
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import (
    create_tables,
    truncate_tables,
    load_clean_transactions,
    load_invalid_transactions,
)


def run_pipeline() -> None:
    print("Starting financial ETL pipeline...")

    raw_path = Path(RAW_DATA_PATH)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data file does not exist: {raw_path}")

    print(f"Extracting data from: {raw_path}")
    raw_df = extract_data(str(raw_path))
    print(f"Extracted {len(raw_df)} raw rows.")

    print("Transforming data...")
    clean_df, invalid_df = transform_data(raw_df)
    print(f"Valid rows:   {len(clean_df)}")
    print(f"Invalid rows: {len(invalid_df)}")

    print("Writing processed CSV outputs...")
    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_csv(PROCESSED_DATA_PATH, index=False)
    invalid_df.to_csv(INVALID_DATA_PATH, index=False)

    print("Preparing database tables...")
    create_tables()
    truncate_tables()

    print("Loading clean transactions...")
    clean_loaded = load_clean_transactions(clean_df)

    print("Loading invalid transactions...")
    invalid_loaded = load_invalid_transactions(invalid_df)

    print("\nPipeline completed successfully.")
    print("Summary")
    print("-------")
    print(f"Raw rows extracted:        {len(raw_df)}")
    print(f"Clean rows loaded:         {clean_loaded}")
    print(f"Invalid rows loaded:       {invalid_loaded}")
    print(f"Processed CSV output:      {PROCESSED_DATA_PATH}")
    print(f"Invalid CSV output:        {INVALID_DATA_PATH}")


if __name__ == "__main__":
    run_pipeline()