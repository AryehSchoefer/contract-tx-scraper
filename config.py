import os
from dataclasses import dataclass

@dataclass
class Config:
    polygon_url: str | None = os.getenv("POLYGON_POS_URL")
    transactions_csv_path: str | None = os.getenv("TRANSACTIONS_CSV_PATH")
    abi_json_path: str | None = os.getenv("ABI_JSON_PATH")
    max_workers = 5
