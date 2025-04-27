from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from hexbytes import HexBytes
from typing import Dict, Any, List, Optional
import json
import os
import time
import pandas as pd
import concurrent.futures
import argparse

from config import Config
from src.tx_details import decode_transaction_input
from src.output import plot_decoding_success, plot_genesis_transitions_over_time, plot_identity_frequency_bubble_chart, save_results_csv

config = Config()

def process_transaction_task(
    w3: Web3,
    hex_hash_string: str,
    contract_abi: list,
    contract_address_from_config: Optional[str],
    verbose: bool
) -> Dict[str, Any]:
    """
    Task function to fetch transaction, get timestamp, and decode input data.

    Args:
        w3: Web3 client.
        hex_hash_string: Transaction hash string.
        contract_abi: Contract ABI.
        contract_address_from_config: The contract address specified in config (optional).
        verbose: Verbose flag.

    Returns:
        Dictionary containing transaction hash, timestamp, and decoding results.
    """
    result_entry: Dict[str, Any] = {
        "transaction_hash": hex_hash_string,
        "timestamp": None,
        "decoded_function": None,
        "decoded_parameters": None,
        "is_genesis_transition": False, # specific to Privado ID use case, but can be generalized
        "decoding_successful": False,
        "error": None
    }
    tx_hash_bytes = HexBytes(hex_hash_string)

    try:
        tx = w3.eth.get_transaction(tx_hash_bytes)
        if tx is None:
            result_entry["error"] = "Transaction not found"
            if verbose:
                print(f"\nError: Transaction {hex_hash_string} not found.")
            return result_entry

        block_number = tx.get('blockNumber')
        if block_number is None:
             result_entry["error"] = "Transaction is pending (no block number)"
             if verbose:
                 print(f"\nWarning: Transaction {hex_hash_string} is pending (no block number). Cannot get timestamp.")
             return result_entry

        try:
            block = w3.eth.get_block(block_number)
            timestamp = block.get('timestamp')
            if timestamp is None:
                 result_entry["error"] = "Block found but no timestamp"
                 if verbose:
                     print(f"\nError: Block {block_number} for transaction {hex_hash_string} has no timestamp.")
                 return result_entry
            result_entry["timestamp"] = timestamp
        except Exception as e:
             result_entry["error"] = f"Error fetching block or timestamp: {e}"
             if verbose:
                 print(f"\nError fetching block {block_number} for transaction {hex_hash_string}: {e}")
             return result_entry


        address_for_decoding = contract_address_from_config if contract_address_from_config else tx.get('to')

        if not address_for_decoding:
             result_entry["error"] = "Contract address not available for decoding"
             if verbose:
                 print(f"\nError: Contract address not available for decoding transaction {hex_hash_string}.")
             return result_entry

        input_data = tx.get('input')

        decoded_data = decode_transaction_input(w3, input_data, contract_abi, address_for_decoding, verbose)

        if decoded_data:
            function_name, parameters = decoded_data
            result_entry["decoded_function"] = function_name
            result_entry["decoded_parameters"] = parameters
            result_entry["decoding_successful"] = True

            # check for isOldStateGenesis only if parameters are decoded and the key exists
            # this check is specific to the Privado ID use case and could be made configurable
            if parameters and 'isOldStateGenesis' in parameters and parameters['isOldStateGenesis'] is True:
                 result_entry["is_genesis_transition"] = True

        else:
            result_entry["error"] = result_entry.get("error", "Decoding failed or no input data")


    except Exception as e:
        result_entry["error"] = f"Unexpected error during processing: {e}"
        print(f"\nAn unexpected error occurred while processing transaction {hex_hash_string}: {e}")


    return result_entry


def run_analytics(verbose: bool = False):
    """
    Loads configuration, transaction hashes, and ABI, then processes
    each transaction concurrently to decode input data.
    Filters transactions based on configured methods.
    Generates graphics and saves results.

    Args:
        verbose: If True, print more detailed information during processing.
    """
    print("Starting analytics run...")

    if not config.rpc_url or not config.transactions_csv_path or not config.abi_json_path:
        print("Error: Missing required configuration values (RPC_URL, TRANSACTIONS_CSV_PATH, ABI_JSON_PATH). Please set environment variables.")
        return

    try:
        w3 = Web3(Web3.HTTPProvider(config.rpc_url))
        if config.apply_poa_middleware:
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            if verbose:
                print("Applied Geth POA middleware.")

        if not w3.is_connected():
            print(f"Error: Failed to connect to network at {config.rpc_url}. Check RPC_URL.")
            return
        print(f"Connected to network at {config.rpc_url}.")
    except Exception as e:
        print(f"Error connecting to Web3 provider at {config.rpc_url}: {e}")
        return

    transactions_to_process_df: pd.DataFrame = pd.DataFrame()
    try:
        df = pd.read_csv(config.transactions_csv_path)

        if 'Transaction Hash' not in df.columns:
            print(f"Error: CSV file '{config.transactions_csv_path}' does not contain a 'Transaction Hash' column.")
            return
        if 'Method' not in df.columns:
             print(f"Error: CSV file '{config.transactions_csv_path}' does not contain a 'Method' column.")
             return

        if config.methods_to_filter:
            print(f"Filtering transactions by methods: {config.methods_to_filter}")
            df['Method'] = df['Method'].astype(str)
            filtered_df = df[df['Method'].isin(config.methods_to_filter)].copy()
        else:
            print("No methods specified for filtering. Processing all transactions in the CSV.")
            filtered_df = df.copy()

        transactions_to_process_df = filtered_df.copy() # type: ignore
        transactions_to_process_df['Transaction Hash'] = transactions_to_process_df['Transaction Hash'].astype(str)
        transactions_to_process_df = transactions_to_process_df.dropna(subset=['Transaction Hash'])


        print(f"Loaded {len(df)} transactions from {config.transactions_csv_path}.")
        print(f"Filtered down to {len(transactions_to_process_df)} transactions to process.")


    except FileNotFoundError:
        print(f"Error: CSV file not found at '{config.transactions_csv_path}'.")
        return
    except Exception as e:
        print(f"Error reading or filtering CSV file '{config.transactions_csv_path}': {e}")
        return

    contract_abi: list = []
    try:
        with open(config.abi_json_path, 'r') as f:
            contract_abi = json.load(f)
        print(f"Loaded contract ABI from {config.abi_json_path}.")
    except FileNotFoundError:
        print(f"Error: ABI JSON file not found at '{config.abi_json_path}'.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config.abi_json_path}'. Ensure it's valid JSON.")
        return
    except Exception as e:
        print(f"Error reading ABI JSON file '{config.abi_json_path}': {e}")
        return

    raw_results: List[Dict[str, Any]] = []
    print(f"Processing {len(transactions_to_process_df)} transactions concurrently with {config.max_workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_to_hash = {
            executor.submit(
                process_transaction_task,
                w3,
                str(row['Transaction Hash']),
                contract_abi,
                config.contract_address,
                verbose
            ): row['Transaction Hash']
            for _, row in transactions_to_process_df.iterrows()
        }

        completed_count = 0
        for future in concurrent.futures.as_completed(future_to_hash):
            completed_count += 1
            hex_hash_string = future_to_hash[future]

            if not verbose:
                 print(f"Completed {completed_count}/{len(transactions_to_process_df)}: {hex_hash_string}", end='\r')


            try:
                result = future.result()
                raw_results.append(result)
            except Exception as e:
                print(f"\nAn error occurred during task execution for hash {hex_hash_string}: {e}")
                raw_results.append({
                    "transaction_hash": hex_hash_string,
                    "timestamp": None,
                    "decoded_function": None,
                    "decoded_parameters": None,
                    "is_genesis_transition": False,
                    "decoding_successful": False,
                    "error": f"Task execution failed: {e}"
                })


    if not verbose:
        print("\n")

    print("\nAnalytics run complete.")
    print(f"Total transactions processed (including failures): {len(raw_results)}")

    analytics_results_decoded = [r for r in raw_results if r.get("decoding_successful")]
    successful_decodes = len(analytics_results_decoded)
    failed_decodes = len(raw_results) - successful_decodes

    print(f"Successfully decoded input data for {successful_decodes} transactions.")
    print(f"Failed to decode input data for {failed_decodes} transactions.")

    results_dir = "results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
        print(f"Created results directory: {results_dir}")

    current_timestamp = int(time.time())

    plot_decoding_success(successful_decodes, failed_decodes, results_dir, current_timestamp)
    plot_genesis_transitions_over_time(analytics_results_decoded, results_dir, current_timestamp)
    plot_identity_frequency_bubble_chart(analytics_results_decoded, results_dir, current_timestamp)

    save_results_csv(raw_results, results_dir, current_timestamp)

    if verbose:
        print("\n--- Example Raw Results (First 5) ---")
        for _, result in enumerate(raw_results[:5]):
            print(f"Tx Hash: {result.get('transaction_hash')}")
            print(f"  Successful: {result.get('decoding_successful')}")
            print(f"  Timestamp: {result.get('timestamp')}")
            print(f"  Is Genesis: {result.get('is_genesis_transition')}")
            if result.get('decoded_successful'):
                print(f"  Function: {result.get('decoded_function')}")
                print(f"  Params: {result.get('decoded_parameters')}")
            elif 'error' in result:
                 print(f"  Error: {result.get('error')}")
            print("-" * 20)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run blockchain transaction analytics.")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output for detailed processing information."
    )

    args = parser.parse_args()

    run_analytics(verbose=args.verbose)

