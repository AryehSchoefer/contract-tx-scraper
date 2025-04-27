from web3 import Web3
from hexbytes import HexBytes
from typing import Dict, Any, Tuple, Optional, Union

def decode_transaction_input(
    w3: Web3,
    input_data: Union[HexBytes, str, None],
    contract_abi: list,
    contract_address: str,
    verbose: bool = False
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Decodes transaction input data using the contract ABI and address.

    Args:
        w3: An initialized web3.py client instance (used for utilities like checksumming).
        input_data: The raw transaction input data as HexBytes or a hex string.
        contract_abi: The ABI of the contract called in the transaction, as a list.
        contract_address: The address of the contract called in the transaction.
        verbose: If True, print more detailed information during processing.

    Returns:
        A tuple containing the function name (str) and a dictionary of decoded
        parameters (Dict[str, Any]), or None if decoding fails or input data is empty.
    """
    try:
        if not input_data or input_data == '0x' or not contract_address:
            if verbose:
                print("  No input data found or contract address missing for decoding.")
            return None

        try:
            checksum_address = w3.to_checksum_address(contract_address)
            contract = w3.eth.contract(address=checksum_address, abi=contract_abi)
            if verbose:
                print("  Contract instance created for decoding.")
        except Exception as e:
            print(f"Error creating contract instance for address {contract_address}. Check contract address and ABI: {e}")
            return None

        try:
            input_bytes = HexBytes(input_data) if isinstance(input_data, str) else input_data
            func_obj, func_params = contract.decode_function_input(input_bytes)

            if verbose:
                print(f"\n  --- Decoded Input Data ---")
                print(f"  Function Called: {func_obj.fn_name}")
                print("  Parameters:")
                for name, value in func_params.items():
                    print(f"    {name}: {value}")
                print("  ------------------------")

            return func_obj.fn_name, func_params

        except Exception as e:
            # this error often means the ABI doesn't match the function called
            if verbose:
                print(f"  Error decoding input data using the provided ABI for tx (hash not available here): {e}")
            return None

    except Exception as e:
        print(f"An unexpected error occurred during decoding: {e}")
        return None

