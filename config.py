import os
from dataclasses import dataclass, field
from typing import List

@dataclass
class Config:
    """
    Configuration class for blockchain analytics script.
    Loads settings from environment variables.
    """
    # RPC URL for the blockchain network (e.g., Polygon, Ethereum)
    # Environment variable: RPC_URL
    rpc_url: str | None = os.getenv("RPC_URL")

    # Path to the CSV file containing transaction data
    # Environment variable: TRANSACTIONS_CSV_PATH
    transactions_csv_path: str | None = os.getenv("TRANSACTIONS_CSV_PATH")

    # Path to the JSON file containing the contract ABI
    # Environment variable: ABI_JSON_PATH
    abi_json_path: str | None = os.getenv("ABI_JSON_PATH")

    # Address of the smart contract to analyze (optional, but needed for decoding)
    # Environment variable: CONTRACT_ADDRESS
    contract_address: str | None = os.getenv("CONTRACT_ADDRESS")

    # List of method names to filter transactions by (e.g., ["Transit State"])
    # If empty or None, no method filtering is applied.
    # Environment variable: METHODS_TO_FILTER (comma-separated string, e.g., "Method1,Method2")
    methods_to_filter: List[str] = field(default_factory=lambda: os.getenv("METHODS_TO_FILTER", "").split(',') if os.getenv("METHODS_TO_FILTER") else [])

    # Maximum number of concurrent workers for fetching data
    # Environment variable: MAX_WORKERS (integer string, e.g., "10")
    max_workers: int = int(os.getenv("MAX_WORKERS", "6"))

    # Flag to indicate if Proof-of-Authority middleware should be applied
    # Environment variable: APPLY_POA_MIDDLEWARE ("True" or "False")
    apply_poa_middleware: bool = os.getenv("APPLY_POA_MIDDLEWARE", "False").lower() == "true"

    def __post_init__(self):
        self.methods_to_filter = [method.strip() for method in self.methods_to_filter if method.strip()]
