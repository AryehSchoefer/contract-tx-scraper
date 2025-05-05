import os
from dataclasses import dataclass, field
from typing import List
from web3 import Web3

@dataclass
class Config:
    """
    Configuration class for blockchain analytics script.
    Loads settings from environment variables.
    Supports different analysis modes.
    """
    # Analysis mode: 'privado', 'civic', or 'worldid'
    # Environment variable: ANALYSIS_MODE (e.g., "privado", "civic", or "worldid")
    analysis_mode: str = os.getenv("ANALYSIS_MODE", "privado").lower() # Default to 'privado'

    # RPC URL for the blockchain network (e.g., Polygon, Ethereum)
    # Environment variable: RPC_URL
    rpc_url: str | None = os.getenv("RPC_URL")

    # Path to the CSV file containing transaction data
    # Environment variable: TRANSACTIONS_CSV_PATH
    transactions_csv_path: str | None = os.getenv("TRANSACTIONS_CSV_PATH")

    # Path to the JSON file containing the contract ABI
    # This ABI should be relevant to the selected ANALYSIS_MODE
    # (e.g., include 'Transit State' function for 'privado', 'Register Identities' function for 'worldid')
    # Environment variable: ABI_JSON_PATH
    abi_json_path: str | None = os.getenv("ABI_JSON_PATH")

    # Address of the smart contract relevant to the analysis mode
    # (e.g., Privado ID State contract for 'privado', Civic Gateway for 'civic',
    # World ID contract for 'worldid')
    # Environment variable: CONTRACT_ADDRESS
    contract_address: str | None = os.getenv("CONTRACT_ADDRESS")

    # List of method names to filter transactions by from the CSV 'Method' column (optional)
    # If empty or None, no method filtering is applied based on CSV column.
    # Relevant for all modes, but Civic, if you want to pre-filter the CSV.
    # Environment variable: METHODS_TO_FILTER (comma-separated string, e.g., "Method1,Method2")
    methods_to_filter: List[str] = field(default_factory=lambda: os.getenv("METHODS_TO_FILTER", "").split(',') if os.getenv("METHODS_TO_FILTER") else [])

    # Maximum number of concurrent workers for fetching data
    # Environment variable: MAX_WORKERS (integer string, e.g., "10")
    max_workers: int = int(os.getenv("MAX_WORKERS", "6"))

    # Flag to indicate if Proof-of-Authority middleware should be applied
    # Set to "True" or "False" in environment variables
    # Environment variable: APPLY_POA_MIDDLEWARE ("True" or "False")
    apply_poa_middleware: bool = os.getenv("APPLY_POA_MIDDLEWARE", "False").lower() == "true"

    # Address representing the null address for minting events (ERC-721 standard)
    # Relevant for 'civic' mode.
    # Environment variable: NULL_ADDRESS (defaults to Ethereum zero address)
    null_address: str = os.getenv("NULL_ADDRESS", "0x0000000000000000000000000000000000000000")

    # Specific method name for identity creation in Privado ID mode
    # Environment variable: PRIVADO_GENESIS_METHOD (e.g., "transitState")
    privado_genesis_method: str = os.getenv("PRIVADO_GENESIS_METHOD", "transitState")

    # Specific method name for identity creation in World ID mode
    # Environment variable: WORLDID_REGISTER_METHOD (e.g., "registerIdentities")
    worldid_register_method: str = os.getenv("WORLDID_REGISTER_METHOD", "registerIdentities")


    def __post_init__(self):
        self.methods_to_filter = [method.strip() for method in self.methods_to_filter if method.strip()]
        if self.null_address:
             try:
                 w3_temp = Web3()
                 self.null_address = w3_temp.to_checksum_address(self.null_address)
             except Exception as e:
                 print(f"Warning: Could not checksum NULL_ADDRESS '{self.null_address}': {e}. Using as is.")

        valid_modes = ['privado', 'civic', 'worldid']
        if self.analysis_mode not in valid_modes:
            print(f"Warning: Invalid ANALYSIS_MODE '{self.analysis_mode}'. Defaulting to 'privado'.")
            self.analysis_mode = 'privado'


