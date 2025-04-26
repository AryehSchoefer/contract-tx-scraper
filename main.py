from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from hexbytes import HexBytes
from typing import Dict, Any, List
import json
import os
import time
import pandas as pd
import concurrent.futures
import argparse
import matplotlib.pyplot as plt

from config import Config
from src.tx_details import get_input_data_tx

config = Config();

def process_transaction_task(
    w3: Web3,
    hex_hash_string: str,
    contract_abi: list,
    verbose: bool
) -> Dict[str, Any]:
    """
    Task function to fetch transaction, get timestamp, and decode input data.

    Args:
        w3: Web3 client.
        hex_hash_string: Transaction hash string.
        contract_abi: Contract ABI.
        verbose: Verbose flag.

    Returns:
        Dictionary containing transaction hash, timestamp, and decoding results.
    """
    result_entry: Dict[str, Any] = {
        "transaction_hash": hex_hash_string,
        "timestamp": None,
        "decoded_function": None,
        "decoded_parameters": None,
        "is_genesis_transition": False,
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

        block = w3.eth.get_block(block_number)
        timestamp = block.get('timestamp')
        if timestamp is None:
             result_entry["error"] = "Block found but no timestamp"
             if verbose:
                 print(f"\nError: Block {block_number} for transaction {hex_hash_string} has no timestamp.")
             return result_entry

        result_entry["timestamp"] = timestamp

        decoded_data = get_input_data_tx(w3, tx_hash_bytes, contract_abi, verbose)

        if decoded_data:
            function_name, parameters = decoded_data
            result_entry["decoded_function"] = function_name
            result_entry["decoded_parameters"] = parameters
            result_entry["decoding_successful"] = True

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
    each transaction concurrently to decode input data using get_input_data_tx.
    Filters transactions to only include those with 'Method' == 'Transit State'.
    Also generates a graphic representation of the results.
    Includes a verbose option for detailed logging, controlled by a CLI argument.

    Args:
        verbose: If True, print more detailed information during processing.
    """
    print("Starting analytics run...")

    config = Config()
    rpc_url = config.polygon_url
    transactions_csv_path = config.transactions_csv_path
    abi_json_path = config.abi_json_path
    max_workers = config.max_workers

    if (not rpc_url or not transactions_csv_path or not abi_json_path):
        print("Error: Missing configuration values. Please check your environment variables.")
        return

    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
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

    raw_results: List[Dict[str, Any]] = []
    print(f"Processing filtered transactions concurrently with {max_workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hash = {
            executor.submit(process_transaction_task, w3, hex_hash_string, contract_abi, verbose): hex_hash_string
            for hex_hash_string in transaction_hashes_to_process
        }

        completed_count = 0
        for future in concurrent.futures.as_completed(future_to_hash):
            completed_count += 1
            hex_hash_string = future_to_hash[future]

            if not verbose:
                 print(f"Completed {completed_count}/{len(transaction_hashes_to_process)}: {hex_hash_string}", end='\r')


            try:
                result = future.result()
                raw_results.append(result)
            except Exception as e:
                print(f"\nAn error occurred retrieving result for hash {hex_hash_string}: {e}")
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

    analytics_results = [r for r in raw_results if r["decoding_successful"]]
    successful_decodes = len(analytics_results)
    failed_decodes = len(raw_results) - successful_decodes

    print(f"Successfully decoded input data for {successful_decodes} transactions.")
    print(f"Failed to decode input data for {failed_decodes} transactions.")

    results_dir = "results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
        print(f"Created results directory: {results_dir}")

    current_timestamp = int(time.time())

    print("\nGenerating Decoding Success graphic...")

    labels = ['Successful Decodes', 'Failed Decodes']
    counts = [successful_decodes, failed_decodes]
    colors = ['#4CAF50', '#F44336']

    plt.figure(figsize=(8, 6))
    plt.bar(labels, counts, color=colors)
    plt.ylabel('Number of Transactions')
    plt.title('Transaction Input Data Decoding Results')
    plt.ylim(0, max(counts) * 1.1)

    for i, count in enumerate(counts):
        plt.text(i, count + (max(counts) * 0.02), str(count), ha='center')

    plt.tight_layout()

    decoding_chart_filename = f"{current_timestamp}_decoding_results_bar_chart.png"
    decoding_chart_path = os.path.join(results_dir, decoding_chart_filename)
    try:
        plt.savefig(decoding_chart_path)
        print(f"Decoding success bar chart saved to {decoding_chart_path}")
    except Exception as e:
        print(f"Error saving decoding success chart: {e}")
    plt.close()

    print("\nGenerating Genesis Transitions Over Time graphic...")

    genesis_transitions = [r for r in analytics_results if r["is_genesis_transition"]]

    if not genesis_transitions:
        print("No genesis transition transactions found to plot over time.")
    else:
        genesis_df = pd.DataFrame(genesis_transitions)

        genesis_df['datetime'] = pd.to_datetime(genesis_df['timestamp'], unit='s')

        genesis_df = genesis_df.sort_values(by='datetime')

        genesis_df['cumulative_count'] = range(1, len(genesis_df) + 1)

        plt.figure(figsize=(12, 6))
        plt.plot(genesis_df['datetime'], genesis_df['cumulative_count'], marker='o', linestyle='-')
        plt.xlabel('Time')
        plt.ylabel('Cumulative Count of Genesis Transitions')
        plt.title('Cumulative Genesis Identity Transitions Over Time')
        plt.grid(True)

        plt.gcf().autofmt_xdate()

        plt.tight_layout()

        genesis_chart_filename = f"{current_timestamp}_genesis_transitions_over_time.png"
        genesis_chart_path = os.path.join(results_dir, genesis_chart_filename)
        try:
            plt.savefig(genesis_chart_path)
            print(f"Genesis transitions chart saved to {genesis_chart_path}")
        except Exception as e:
            print(f"Error saving genesis transitions chart: {e}")
        plt.close()

    try:
        results_df = pd.DataFrame(raw_results)
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
