from web3 import Web3
from hexbytes import HexBytes
from typing import Dict, Any, List
import json
import pandas as pd
import concurrent.futures
import argparse

from config import Config
from src.tx_details import get_input_data_tx

config = Config();

def run_analytics(verbose: bool = False):
    """
    Loads configuration, transaction hashes, and ABI, then processes
    each transaction concurrently to decode input data using get_input_data_tx.
    Filters transactions to only include those with 'Method' == 'Transit State'.
    Also generates a graphic representation of the results.
    Includes a verbose option for detailed logging, controlled by a CLI argument.

    Args:
        verbose: If True, print more detailed information during processing.
    """
    print("Starting analytics run...")

    rpc_url = config.polygon_url
    transactions_csv_path = config.transactions_csv_path
    abi_json_path = config.abi_json_path
    max_workers = config.max_workers

    if (not rpc_url or not transactions_csv_path or not abi_json_path):
        print("Error: Missing configuration values. Please check your environment variables.")
        return

    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            print(f"Error: Failed to connect to Polygon network at {rpc_url}. Check RPC_URL.")
            return
        print("Connected to Polygon network.")
    except Exception as e:
        print(f"Error connecting to Web3 provider at {rpc_url}: {e}")
        return

    transaction_hashes_to_process: List[str] = []
    try:
        df = pd.read_csv(transactions_csv_path)

        if 'Transaction Hash' not in df.columns:
            print(f"Error: CSV file '{transactions_csv_path}' does not contain a 'Transaction Hash' column.")
            return
        if 'Method' not in df.columns:
             print(f"Error: CSV file '{transactions_csv_path}' does not contain a 'Method' column.")
             return

        filtered_df = df[df['Method'] == 'Transit State'].copy()

        transaction_hashes_to_process = filtered_df['Transaction Hash'].dropna().tolist() # type: ignore

        print(f"Loaded {len(df)} transactions from {transactions_csv_path}.")
        print(f"Filtered down to {len(transaction_hashes_to_process)} transactions with Method 'Transit State'.")


    except FileNotFoundError:
        print(f"Error: CSV file not found at '{transactions_csv_path}'.")
        return
    except Exception as e:
        print(f"Error reading or filtering CSV file '{transactions_csv_path}': {e}")
        return

    contract_abi: list = []
    try:
        with open(abi_json_path, 'r') as f:
            contract_abi = json.load(f)
        print(f"Loaded contract ABI from {abi_json_path}.")
    except FileNotFoundError:
        print(f"Error: ABI JSON file not found at '{abi_json_path}'.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{abi_json_path}'. Ensure it's valid JSON.")
        return
    except Exception as e:
        print(f"Error reading ABI JSON file '{abi_json_path}': {e}")
        return

    analytics_results: List[Dict[str, Any]] = []
    print(f"Processing filtered transactions concurrently with {max_workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hash = {
            executor.submit(get_input_data_tx, w3, HexBytes(hex_hash_string), contract_abi, verbose): hex_hash_string
            for hex_hash_string in transaction_hashes_to_process
        }

        completed_count = 0
        for future in concurrent.futures.as_completed(future_to_hash):
            completed_count += 1
            hex_hash_string = future_to_hash[future]

            if not verbose:
                 print(f"Completed {completed_count}/{len(transaction_hashes_to_process)}: {hex_hash_string}", end='\r')


            try:
                decoded_data = future.result()

                result_entry: Dict[str, Any] = {
                    "transaction_hash": hex_hash_string,
                    "decoded_function": None,
                    "decoded_parameters": None,
                    "decoding_successful": False
                }

                if decoded_data:
                    function_name, parameters = decoded_data
                    result_entry["decoded_function"] = function_name
                    result_entry["decoded_parameters"] = parameters
                    result_entry["decoding_successful"] = True
                    if verbose:
                         print(f"\n  Successfully decoded: {function_name}")
                else:
                    if verbose:
                        print(f"\n  Decoding failed or no input data.")
                    pass

                analytics_results.append(result_entry)

            except Exception as e:
                print(f"\nAn error occurred processing hash {hex_hash_string} after future completion: {e}")
                analytics_results.append({
                    "transaction_hash": hex_hash_string,
                    "decoded_function": None,
                    "decoded_parameters": None,
                    "decoding_successful": False,
                    "error": str(e)
                })

    if not verbose:
        print("\n")

    print("\nAnalytics run complete.")
    print(f"Processed {len(analytics_results)} transactions.")
    successful_decodes = sum(1 for r in analytics_results if r["decoding_successful"])
    failed_decodes = len(analytics_results) - successful_decodes
    print(f"Successfully decoded input data for {successful_decodes} transactions.")
    print(f"Failed to decode input data for {failed_decodes} transactions.")

    try:
        results_df = pd.DataFrame(analytics_results)
        results_df.to_csv("analytics_results.csv", index=False)
        print("\nAnalytics results saved to analytics_results.csv")
    except Exception as e:
        print(f"Error saving results to CSV: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run blockchain transaction analytics.")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output for detailed processing information."
    )

    args = parser.parse_args()

    run_analytics(verbose=args.verbose)

