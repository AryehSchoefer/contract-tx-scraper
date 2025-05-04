from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from hexbytes import HexBytes
from typing import Dict, Any, List, Optional, Union
import json
import os
import time
import pandas as pd
import concurrent.futures
import argparse

from config import Config
from src.tx_details import decode_transaction_input
from src.output import (
    plot_privado_decoding_success,
    plot_privado_genesis_cumulative,
    plot_privado_genesis_daily,
    plot_privado_identity_frequency_bubble_chart,
    plot_civic_minting_success,
    plot_civic_cumulative_minted_tokens_over_time,
    plot_civic_daily_minted_tokens,
    plot_civic_recipient_address_frequency_bubble_chart,
    save_results_csv
)

config = Config()

def process_transaction_task(
    w3: Web3,
    hex_hash_string: str,
    contract_abi: list,
    contract_address_from_config: Optional[str],
    null_address: str, # needed for Civic mode
    analysis_mode: str,
    verbose: bool
) -> Dict[str, Any]:
    """
    Task function to fetch transaction, get timestamp, and process based on analysis mode.

    Args:
        w3: Web3 client.
        hex_hash_string: Transaction hash string.
        contract_abi: Contract ABI relevant to the analysis mode.
        contract_address_from_config: The contract address specified in config (optional).
        null_address: The configured null address (0x0...0), used in 'civic' mode.
        analysis_mode: The selected analysis mode ('privado' or 'civic').
        verbose: Verbose flag.

    Returns:
        Dictionary containing transaction hash, timestamp, and analysis results based on mode.
    """
    result_entry: Dict[str, Any] = {
        "transaction_hash": hex_hash_string,
        "timestamp": None,
        # fields for Privado mode
        "decoded_function": None,
        "decoded_parameters": None,
        "is_genesis_transition": False,
        "privado_decoding_successful": False, # specific success flag for Privado decoding
        # fields for Civic mode
        "is_minting_event": False,
        "recipient_address": None,
        "token_id": None,
        "civic_log_processing_successful": False, # specific success flag for Civic log processing
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

        address_for_mode_processing = contract_address_from_config if contract_address_from_config else tx.get('to')

        if not address_for_mode_processing:
             result_entry["error"] = "Contract address not available for processing in this mode"
             if verbose:
                 print(f"\nError: Contract address not available for processing transaction {hex_hash_string} in mode '{analysis_mode}'.")
             return result_entry


        # --- Process based on Analysis Mode ---
        if analysis_mode == 'privado':
            # Privado ID Mode: decode input data and check for genesis transition
            input_data = tx.get('input')
            input_data_for_decoding: Union[HexBytes, str, None] = input_data

            decoded_data = decode_transaction_input(w3, input_data_for_decoding, contract_abi, address_for_mode_processing, verbose)

            if decoded_data:
                function_name, parameters = decoded_data
                result_entry["decoded_function"] = function_name
                result_entry["decoded_parameters"] = parameters
                result_entry["privado_decoding_successful"] = True

                # check for isOldStateGenesis only if parameters are decoded and the key exists
                if parameters and 'isOldStateGenesis' in parameters and parameters['isOldStateGenesis'] is True:
                     result_entry["is_genesis_transition"] = True

            else:
                # if decoding failed, the error might already be set in decode_transaction_input
                result_entry["error"] = result_entry.get("error", "Privado decoding failed or no input data")


        elif analysis_mode == 'civic':
            # Civic Mode: Process event logs to find minting events
            try:
                receipt = w3.eth.get_transaction_receipt(tx_hash_bytes)
                transfer_event_signature = w3.keccak(text="Transfer(address,address,uint256)").hex()
                for log in receipt["logs"]:
                    if log['topics'][0].hex() == transfer_event_signature:
                        from_address = w3.to_checksum_address(log['topics'][1].hex()[-40:])
                        if from_address == null_address:
                            result_entry["is_minting_event"] = True
                            result_entry["civic_log_processing_successful"] = True
                            result_entry["recipient_address"] = w3.to_checksum_address(log['topics'][2].hex()[-40:])
                            result_entry["token_id"] = int(log['topics'][3].hex(), 16)

                if not result_entry["is_minting_event"]:
                     result_entry["error"] = result_entry.get("error", "No minting event found in logs")

            except Exception as e:
                result_entry["error"] = f"Error processing event logs: {e}"
                if verbose:
                    print(f"\nError processing event logs for transaction {hex_hash_string}: {e}")
        else:
            result_entry["error"] = f"Unsupported analysis mode: {analysis_mode}"
            print(f"\nError: Unsupported analysis mode '{analysis_mode}' for transaction {hex_hash_string}.")

    except Exception as e:
        result_entry["error"] = f"Unexpected error during transaction fetch or initial processing: {e}"
        print(f"\nAn unexpected error occurred while processing transaction {hex_hash_string}: {e}")


    return result_entry


def run_analytics(verbose: bool = False):
    """
    Loads configuration, transaction hashes, and ABI, then processes
    each transaction concurrently based on the selected analysis mode.
    Filters transactions based on configured methods (optional).
    Generates graphics and saves results.

    Args:
        verbose: If True, print more detailed information during processing.
    """
    start_time = time.time()

    print("Starting analytics run...")
    print(f"Analysis Mode: {config.analysis_mode.capitalize()}")

    # required_config = ["rpc_url", "transactions_csv_path", "abi_json_path", "contract_address"]
    required_config = ["rpc_url", "transactions_csv_path", "abi_json_path"]
    if not all(getattr(config, attr) for attr in required_config):
        missing = [attr for attr in required_config if not getattr(config, attr)]
        print(f"Error: Missing required configuration values: {', '.join(missing)}. Please set environment variables.")
        return


    # needed for type checking
    assert config.transactions_csv_path is not None, "transactions_csv_path must be set after config validation"
    # assert config.contract_address is not None, "contract_address must be set after config validation"
    assert config.abi_json_path is not None, "abi_json_path must be set after config validation"
    assert config.rpc_url is not None, "rpc_url must be set after config validation"

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
        df = pd.read_csv(config.transactions_csv_path, index_col=False)

        if 'Transaction Hash' not in df.columns:
            print(f"Error: CSV file '{config.transactions_csv_path}' does not contain a 'Transaction Hash' column.")
            return
        if config.analysis_mode == 'Civic' and 'Method' not in df.columns:
             print(f"Error: CSV file '{config.transactions_csv_path}' does not contain a 'Method' column.")
             return

        if config.methods_to_filter:
            print(f"Filtering transactions by methods: {config.methods_to_filter}")
            df['Method'] = df['Method'].astype(str)
            filtered_df = df[df['Method'].isin(config.methods_to_filter)].copy()
        else:
            print("No methods specified for filtering. Processing all transactions in the CSV.")
            filtered_df = df.copy()

        transactions_to_process_df = filtered_df.copy()
        transactions_to_process_df['Transaction Hash'] = transactions_to_process_df['Transaction Hash'].astype(str)
        transactions_to_process_df: pd.DataFrame = transactions_to_process_df.dropna(subset=['Transaction Hash'])

        initial_row_count = len(transactions_to_process_df)
        transactions_to_process_df.drop_duplicates(subset=['Transaction Hash'], inplace=True)
        deduplicated_row_count = len(transactions_to_process_df)

        if initial_row_count > deduplicated_row_count:
            print(f"Removed {initial_row_count - deduplicated_row_count} duplicate transaction hashes.")

        print(f"Loaded {initial_row_count} transactions from {config.transactions_csv_path}.")
        print(f"Filtered down to {len(filtered_df)} transactions before deduplication.")
        print(f"Processing {deduplicated_row_count} unique transactions after filtering and deduplication.")



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
    except json.JSONDecodeError as e:
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
                config.null_address,
                config.analysis_mode,
                verbose
            ): str(row['Transaction Hash'])
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
                failed_entry: Dict[str, Any] = {
                    "transaction_hash": hex_hash_string,
                    "timestamp": None,
                    "decoded_function": None,
                    "decoded_parameters": None,
                    "is_genesis_transition": False,
                    "privado_decoding_successful": False,
                    "is_minting_event": False,
                    "recipient_address": None,
                    "token_id": None,
                    "civic_log_processing_successful": False,
                    "error": f"Task execution failed: {e}"
                }
                raw_results.append(failed_entry)


    if not verbose:
        print("\n")

    print("\nAnalytics run complete.")
    print(f"Total transactions processed (including failures): {len(raw_results)}")

    base_results_dir = "results"
    mode_results_dir = os.path.join(base_results_dir, config.analysis_mode)
    current_timestamp = int(time.time())
    timestamped_results_dir = os.path.join(mode_results_dir, str(current_timestamp))

    if not os.path.exists(timestamped_results_dir):
        os.makedirs(timestamped_results_dir)
        print(f"Created results directory: {timestamped_results_dir}")


    if config.analysis_mode == 'privado':
        privado_decoded_results = [r for r in raw_results if r.get("privado_decoding_successful")]
        successful_count = len(privado_decoded_results)
        failed_count = len(raw_results) - successful_count

        plot_privado_decoding_success(successful_count, failed_count, timestamped_results_dir, current_timestamp)
        plot_privado_genesis_cumulative(privado_decoded_results, timestamped_results_dir, current_timestamp)
        plot_privado_genesis_daily(privado_decoded_results, timestamped_results_dir, current_timestamp)
        plot_privado_identity_frequency_bubble_chart(privado_decoded_results, timestamped_results_dir, current_timestamp)

    elif config.analysis_mode == 'civic':
        civic_minting_results = [r for r in raw_results if r.get("is_minting_event")]
        successful_count = len(civic_minting_results)
        failed_count = len(raw_results) - successful_count

        plot_civic_minting_success(successful_count, failed_count, timestamped_results_dir, current_timestamp)
        plot_civic_cumulative_minted_tokens_over_time(civic_minting_results, timestamped_results_dir, current_timestamp)
        plot_civic_daily_minted_tokens(civic_minting_results, timestamped_results_dir, current_timestamp)
        plot_civic_recipient_address_frequency_bubble_chart(civic_minting_results, timestamped_results_dir, current_timestamp)

    save_results_csv(raw_results, timestamped_results_dir, current_timestamp)


    if verbose:
        print("\n--- Example Raw Results (First 5) ---")
        for _, result in enumerate(raw_results[:5]):
            print(f"Tx Hash: {result.get('transaction_hash')}")
            print(f"  Timestamp: {result.get('timestamp')}")
            if result.get('error'):
                 print(f"  Error: {result.get('error')}")
            if config.analysis_mode == 'privado':
                 print(f"  Privado Decoding Successful: {result.get('privado_decoding_successful')}")
                 print(f"  Is Genesis Transition: {result.get('is_genesis_transition')}")
                 if result.get('privado_decoding_successful'):
                      print(f"  Function: {result.get('decoded_function')}")
                      print(f"  Params: {result.get('decoded_parameters')}")
            elif config.analysis_mode == 'civic':
                 print(f"  Civic Log Processing Successful: {result.get('civic_log_processing_successful')}")
                 print(f"  Is Minting Event: {result.get('is_minting_event')}")
                 if result.get('is_minting_event'):
                      print(f"  Recipient: {result.get('recipient_address')}")
                      print(f"  Token ID: {result.get('token_id')}")
            print("-" * 20)

    end_time = time.time()
    total_duration = end_time - start_time
    print(f"\nTotal script execution time: {total_duration:.2f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run blockchain transaction analytics.")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output for detailed processing information."
    )

    args = parser.parse_args()

    run_analytics(verbose=args.verbose)

