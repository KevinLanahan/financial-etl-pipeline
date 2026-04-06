import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

RAW_DATA_PATH = BASE_DIR / "data" / "raw" / "transactions.csv"
PROCESSED_DATA_PATH = BASE_DIR / "data" / "processed" / "cleaned_transactions.csv"
INVALID_DATA_PATH = BASE_DIR / "data" / "processed" / "invalid_transactions.csv"


def load_env_file(env_path: Path = BASE_DIR / ".env") -> None:
    """
    Lightweight .env loader so we don't need python-dotenv.
    Expected format:
        DB_HOST=localhost
        DB_PORT=5432
        DB_NAME=financial_etl
        DB_USER=postgres
        DB_PASSWORD=yourpassword
    """
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_env_file()


@dataclass
class DatabaseConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", 5432))
    name: str = os.getenv("DB_NAME", "financial_etl")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "postgres")

    @property
    def connection_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.name,
            "user": self.user,
            "password": self.password,
        }


DB_CONFIG = DatabaseConfig()