# Financial Transaction ETL Pipeline

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for processing financial transaction data. It simulates how raw transactional data is ingested, cleaned, validated, and stored in a relational database for downstream analytics.

The pipeline is designed to reflect real-world data engineering workflows commonly used in financial systems, analytics platforms, and enterprise data pipelines.

---

## Tech Stack

* Python
* Pandas (data processing & transformation)
* PostgreSQL (data storage)
* psycopg2 (database connectivity)
* SQL

---

## Architecture

Raw CSV Data
↓
[Extract Layer]
↓
[Transform Layer]
↓
[Load Layer → PostgreSQL]
↓
[SQL Analytics / Reporting]

---

## Project Structure

financial-etl-pipeline/
│
├── data/
│   ├── raw/
│   │   └── transactions.csv
│   └── processed/
│
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── pipeline.py
│
├── sql/
│   ├── schema.sql
│   └── queries.sql
│
├── config.py
├── requirements.txt
├── README.md
└── .env.example

---

## ETL Pipeline Stages

### 1. Extract

* Reads raw transaction data from a CSV file
* Validates required schema fields
* Handles missing or malformed input files

---

### 2. Transform

The transformation layer performs several key operations:

#### Data Cleaning

* Removes duplicate transactions
* Handles missing values
* Standardizes text formatting

#### Normalization

* Standardizes merchant names (e.g., AMZN, Amazon.com → Amazon)
* Normalizes categories and payment methods
* Standardizes transaction statuses

#### Validation

* Detects invalid dates and amounts
* Flags missing account identifiers
* Separates invalid records into a dedicated dataset

#### Feature Engineering

* Transaction month and year
* Debit/Credit classification
* Absolute transaction amount
* High-value transaction flag
* Weekend transaction indicator
* Merchant frequency counts

---

### 3. Load

* Creates database tables programmatically
* Loads clean data into clean_transactions
* Loads invalid data into invalid_transactions
* Supports rerunnable pipeline execution via table truncation

---

## Database Tables

### clean_transactions

Contains validated and transformed transaction data.

### invalid_transactions

Stores rejected records along with error reasons for debugging and auditing.

---

## Running the Pipeline

### 1. Install dependencies

pip3 install -r requirements.txt

### 2. Configure environment variables

Create a .env file using .env.example:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=financial_etl
DB_USER=your_username
DB_PASSWORD=your_password

### 3. Run the pipeline

python3 -m etl.pipeline

---

## Sample Output

Extracted 26 raw rows
Valid rows: 22
Invalid rows: 3

Pipeline completed successfully.

---

## Example Queries

-- Total spending by category
SELECT category, SUM(amount)
FROM clean_transactions
GROUP BY category;

-- Monthly spending trends
SELECT transaction_year, transaction_month, SUM(amount)
FROM clean_transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;

-- High-value transactions
SELECT *
FROM clean_transactions
WHERE high_value_transaction = TRUE;

---
