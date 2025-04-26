from web3 import Web3
from hexbytes import HexBytes
from typing import Dict, Any, Tuple, Optional

def get_input_data_tx(
    w3: Web3,
    tx_hash: HexBytes,
    contract_abi: list,
    verbose: bool = False
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Retrieves a transaction by hash and decodes its input data using the contract ABI.

    Args:
        w3: An initialized web3.py client instance connected to the network.
        tx_hash: The transaction hash as a HexBytes object.
        contract_abi: The ABI of the contract called in the transaction, as a list.
        verbose: If True, print more detailed information during processing.

    Returns:
        A tuple containing the function name (str) and a dictionary of decoded
        parameters (Dict[str, Any]), or None if the transaction has no input
        data or decoding fails.
    """
    try:
        tx = w3.eth.get_transaction(tx_hash)
        if tx is None:
            if verbose:
                 print(f"Error: Transaction with hash {tx_hash.hex()} not found.")
            return None

        if verbose:
            print(f"Processing transaction: {tx_hash.hex()}")
            print(f"  From: {tx.get('from')}")
            print(f"  To: {tx.get('to')}")

        input_data = tx.get('input')
        contract_address = tx.get('to')

        if not input_data or input_data == '0x' or not contract_address:
            if verbose:
                print("No input data found for this transaction.")
            return None

        try:
            checksum_address = w3.to_checksum_address(contract_address)
            contract = w3.eth.contract(address=checksum_address, abi=contract_abi)
            if verbose:
                print("  Contract instance created.")
        except Exception as e:
            print(f"Error creating contract instance for address {contract_address}. Check contract address and ABI: {e}")
            return None

        try:
            func_obj, func_params = contract.decode_function_input(input_data)

            if verbose:
                print(f"\n  --- Decoded Input Data ---")
                print(f"  Function Called: {func_obj.fn_name}")
                print("  Parameters:")
                for name, value in func_params.items():
                    print(f"    {name}: {value}")
                print("  ------------------------")

            return func_obj.fn_name, func_params

        except Exception as e:
            if verbose:
                print(f"  Error decoding input data using the provided ABI: {e}")
                print(f"  Raw Input Data: {input_data}")
            return None

    except Exception as e:
        print(f"An unexpected error occurred while processing transaction {tx_hash.hex()}: {e}")
        return None

