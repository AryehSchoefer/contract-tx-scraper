import matplotlib.pyplot as plt
import pandas as pd
import os
from typing import List, Dict, Any
from collections import Counter
import matplotlib.dates as mdates
import numpy as np

# --- Functions for Privado ID Analysis ---

def plot_privado_decoding_success(successful_count: int, failed_count: int, results_dir: str, timestamp: int):
    """Generates and saves a bar chart for Privado ID input decoding success."""
    print("\nGenerating Privado ID Decoding Success graphic...")

    labels = ['Successful Decodes', 'Failed Decodes']
    counts = [successful_count, failed_count]
    colors = ['#4CAF50', '#F44336']

    plt.figure(figsize=(8, 6))
    plt.bar(labels, counts, color=colors)
    plt.ylabel('Number of Transactions')
    plt.title('Privado ID Input Data Decoding Results')
    plt.ylim(0, max(counts) * 1.1)

    for i, count in enumerate(counts):
        plt.text(i, count + (max(counts) * 0.02), str(count), ha='center')

    plt.tight_layout()
    decoding_chart_filename = f"{timestamp}_privado_decoding_results_bar_chart.png"
    decoding_chart_path = os.path.join(results_dir, decoding_chart_filename)
    try:
        plt.savefig(decoding_chart_path)
        print(f"Privado ID decoding success bar chart saved to {decoding_chart_path}")
    except Exception as e:
        print(f"Error saving Privado ID decoding success chart: {e}")
    plt.close()


def plot_privado_genesis_cumulative(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """Generates and saves a line chart for cumulative Privado ID genesis transitions over time."""
    print("\nGenerating Cumulative Privado ID Genesis Transitions Over Time graphic...")

    genesis_transitions = [
        r for r in results
        if r.get("privado_decoding_successful") and r.get("is_genesis_transition") and r.get("timestamp") is not None
    ]

    if not genesis_transitions:
        print("No Privado ID genesis transition transactions with timestamps found to plot over time.")
        return

    genesis_df = pd.DataFrame(genesis_transitions)

    genesis_df['datetime'] = pd.to_datetime(genesis_df['timestamp'], unit='s')

    genesis_df = genesis_df.sort_values(by='datetime')

    genesis_df['cumulative_count'] = range(1, len(genesis_df) + 1)

    plt.figure(figsize=(12, 6))
    plt.plot(genesis_df['datetime'], genesis_df['cumulative_count'], marker='o', linestyle='-')
    plt.xlabel('Time')
    plt.ylabel('Cumulative Count of Genesis Transitions')
    plt.title('Cumulative Privado ID Genesis Identity Transitions Over Time')
    plt.grid(True)

    plt.gcf().autofmt_xdate()

    plt.tight_layout()
    cumulative_chart_filename = f"{timestamp}_privado_cumulative_genesis_transitions_over_time.png"
    cumulative_chart_path = os.path.join(results_dir, cumulative_chart_filename)
    try:
        plt.savefig(cumulative_chart_path)
        print(f"Cumulative Privado ID genesis transitions chart saved to {cumulative_chart_path}")
    except Exception as e:
        print(f"Error saving cumulative Privado ID genesis transitions chart: {e}")
    plt.close()


def plot_privado_genesis_daily(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """
    Generates and saves a bar chart showing the number of Privado ID genesis transitions per day.
    """
    print("\nGenerating Daily Privado ID Genesis Transitions graphic...")

    genesis_transitions = [
        r for r in results
        if r.get("privado_decoding_successful") and r.get("is_genesis_transition") and r.get("timestamp") is not None
    ]

    if not genesis_transitions:
        print("No Privado ID genesis transition transactions with timestamps found to plot daily counts.")
        return

    genesis_df = pd.DataFrame(genesis_transitions)

    genesis_df['date'] = pd.to_datetime(genesis_df['timestamp'], unit='s').dt.date

    daily_counts = genesis_df['date'].value_counts().sort_index()

    if daily_counts.empty:
        print("No daily Privado ID genesis transition counts to plot.")
        return

    plt.figure(figsize=(15, 7))
    ax = plt.gca()

    ax.bar(mdates.date2num(daily_counts.index), daily_counts.values, color='skyblue')

    plt.xlabel('Date')
    plt.ylabel('Number of Genesis Transitions')
    plt.title('Daily Privado ID Genesis Identity Transitions Over Time')
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()

    daily_chart_filename = f"{timestamp}_privado_daily_genesis_transitions.png"
    daily_chart_path = os.path.join(results_dir, daily_chart_filename)
    try:
        plt.savefig(daily_chart_path)
        print(f"Daily Privado ID genesis transitions chart saved to {daily_chart_path}")
    except Exception as e:
        print(f"Error saving daily Privado ID genesis transitions chart: {e}")
    plt.close()


def plot_privado_identity_frequency_bubble_chart(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """
    Generates and saves a bubble chart showing the frequency of identity IDs
    in 'transitState' transactions for Privado ID.
    """
    print("\nGenerating Privado ID Identity Frequency graphic...")

    # filter for successfully decoded 'transitState' transactions with 'id' parameter
    transit_state_ids = []
    for r in results:
        if (r.get("privado_decoding_successful") and
            r.get("decoded_function") == "transitState" and
            r.get("decoded_parameters") and
            'id' in r["decoded_parameters"]):
            # convert the ID to string to handle potentially very large integers
            transit_state_ids.append(str(r["decoded_parameters"]["id"]))

    if not transit_state_ids:
        print("No 'transitState' transactions with identity IDs found to plot for Privado ID.")
        return

    id_counts = Counter(transit_state_ids)

    unique_ids = list(id_counts.keys())
    counts = list(id_counts.values())

    sorted_indices = sorted(range(len(counts)), key=lambda k: counts[k], reverse=True)
    sorted_unique_ids = [unique_ids[i] for i in sorted_indices]
    sorted_counts = [counts[i] for i in sorted_indices]

    x_indices = range(len(sorted_unique_ids))

    size_multiplier = 50
    bubble_sizes = [np.log1p(count) * size_multiplier for count in sorted_counts]

    plt.figure(figsize=(15, 7))
    scatter = plt.scatter(
        x_indices,
        sorted_counts,
        s=bubble_sizes,
        alpha=0.6,
        edgecolors="w",
        linewidth=1
    )

    plt.xlabel('Unique Identity ID (Sorted by Frequency)')
    plt.ylabel('Number of Transactions')
    plt.title('Frequency of Identity IDs in Transit State Transactions (Privado ID)')
    plt.grid(True, linestyle='--', alpha=0.6)

    num_ticks = min(20, len(sorted_unique_ids))
    tick_indices = [int(i * len(x_indices) / num_ticks) for i in range(num_ticks)]
    plt.xticks([x_indices[i] for i in tick_indices], [sorted_unique_ids[i][:10] + '...' for i in tick_indices], rotation=45, ha='right')

    plt.tight_layout()
    identity_freq_chart_filename = f"{timestamp}_privado_identity_frequency_bubble_chart.png"
    identity_freq_chart_path = os.path.join(results_dir, identity_freq_chart_filename)
    try:
        plt.savefig(identity_freq_chart_path)
        print(f"Privado ID identity frequency bubble chart saved to {identity_freq_chart_path}")
    except Exception as e:
        print(f"Error saving Privado ID identity frequency chart: {e}")
    plt.close()


# --- Functions for Civic Analysis ---

def plot_civic_minting_success(successful_count: int, failed_count: int, results_dir: str, timestamp: int):
    """Generates and saves a bar chart for Civic minting event identification success."""
    print("\nGenerating Civic Minting Event Identification graphic...")

    labels = ['Minting Events Found', 'No Minting Event Found / Error']
    counts = [successful_count, failed_count]
    colors = ['#4CAF50', '#F44336']

    plt.figure(figsize=(8, 6))
    plt.bar(labels, counts, color=colors)
    plt.ylabel('Number of Transactions Processed')
    plt.title('Civic Minting Event Identification Results')
    plt.ylim(0, max(counts) * 1.1)

    for i, count in enumerate(counts):
        plt.text(i, count + (max(counts) * 0.02), str(count), ha='center')

    plt.tight_layout()
    minting_chart_filename = f"{timestamp}_civic_minting_identification_bar_chart.png"
    minting_chart_path = os.path.join(results_dir, minting_chart_filename)
    try:
        plt.savefig(minting_chart_path)
        print(f"Civic minting identification bar chart saved to {minting_chart_path}")
    except Exception as e:
        print(f"Error saving Civic minting identification chart: {e}")
    plt.close()


def plot_civic_cumulative_minted_tokens_over_time(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """Generates and saves a line chart for cumulative Civic minted tokens over time."""
    print("\nGenerating Cumulative Civic Minted Tokens Over Time graphic...")

    minting_events = [
        r for r in results
        if r.get("is_minting_event") and r.get("timestamp") is not None
    ]

    if not minting_events:
        print("No Civic minting events with timestamps found to plot over time.")
        return

    minting_df = pd.DataFrame(minting_events)

    minting_df['datetime'] = pd.to_datetime(minting_df['timestamp'], unit='s')

    minting_df = minting_df.sort_values(by='datetime')

    minting_df['cumulative_count'] = range(1, len(minting_df) + 1)

    plt.figure(figsize=(12, 6))
    plt.plot(minting_df['datetime'], minting_df['cumulative_count'], marker='o', linestyle='-')
    plt.xlabel('Time')
    plt.ylabel('Cumulative Count of Minted Tokens')
    plt.title('Cumulative Civic Minted Tokens Over Time')
    plt.grid(True)

    plt.gcf().autofmt_xdate()

    plt.tight_layout()
    cumulative_chart_filename = f"{timestamp}_civic_cumulative_minted_tokens_over_time.png"
    cumulative_chart_path = os.path.join(results_dir, cumulative_chart_filename)
    try:
        plt.savefig(cumulative_chart_path)
        print(f"Cumulative Civic minted tokens chart saved to {cumulative_chart_path}")
    except Exception as e:
        print(f"Error saving cumulative Civic minted tokens chart: {e}")
    plt.close()


def plot_civic_daily_minted_tokens(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """
    Generates and saves a bar chart showing the number of Civic minted tokens per day.
    """
    print("\nGenerating Daily Civic Minted Tokens graphic...")

    minting_events = [
        r for r in results
        if r.get("is_minting_event") and r.get("timestamp") is not None
    ]

    if not minting_events:
        print("No Civic minting events with timestamps found to plot daily counts.")
        return

    minting_df = pd.DataFrame(minting_events)

    minting_df['date'] = pd.to_datetime(minting_df['timestamp'], unit='s').dt.date

    daily_counts = minting_df['date'].value_counts().sort_index()

    if daily_counts.empty:
        print("No daily Civic minted token counts to plot.")
        return

    plt.figure(figsize=(15, 7))
    ax = plt.gca()

    ax.bar(mdates.date2num(daily_counts.index), daily_counts.values, color='skyblue')

    plt.xlabel('Date')
    plt.ylabel('Number of Minted Tokens')
    plt.title('Daily Civic Minted Tokens Over Time')
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()

    daily_chart_filename = f"{timestamp}_civic_daily_minted_tokens.png"
    daily_chart_path = os.path.join(results_dir, daily_chart_filename)
    try:
        plt.savefig(daily_chart_path)
        print(f"Daily Civic minted tokens chart saved to {daily_chart_path}")
    except Exception as e:
        print(f"Error saving daily Civic minted tokens chart: {e}")
    plt.close()


def plot_civic_recipient_address_frequency_bubble_chart(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """
    Generates and saves a bubble chart showing the frequency of recipient addresses
    from Civic minting events.
    """
    print("\nGenerating Civic Recipient Address Frequency graphic...")

    recipient_addresses = [
        r.get("recipient_address") for r in results
        if r.get("is_minting_event") and r.get("recipient_address")
    ]

    if not recipient_addresses:
        print("No recipient addresses found from Civic minting events to plot.")
        return

    address_counts = Counter(recipient_addresses)

    unique_addresses = list(address_counts.keys())
    counts = list(address_counts.values())

    sorted_indices = sorted(range(len(counts)), key=lambda k: counts[k], reverse=True)
    sorted_unique_addresses = [unique_addresses[i] for i in sorted_indices]
    sorted_counts = [counts[i] for i in sorted_indices]

    x_indices = range(len(sorted_unique_addresses))

    size_multiplier = 50
    bubble_sizes = [np.log1p(count) * size_multiplier for count in sorted_counts]

    plt.figure(figsize=(15, 7))
    scatter = plt.scatter(
        x_indices,
        sorted_counts,
        s=bubble_sizes,
        alpha=0.6,
        edgecolors="w",
        linewidth=1
    )

    plt.xlabel('Recipient Address (Sorted by Frequency)')
    plt.ylabel('Number of Minted Tokens Received')
    plt.title('Frequency of Recipient Addresses in Minting Events (Civic)')
    plt.grid(True, linestyle='--', alpha=0.6)

    num_ticks = min(20, len(sorted_unique_addresses))
    tick_indices = [int(i * len(x_indices) / num_ticks) for i in range(num_ticks)]
    plt.xticks([x_indices[i] for i in tick_indices], [sorted_unique_addresses[i][:10] + '...' for i in tick_indices], rotation=45, ha='right') # type: ignore[reportOptionalSubscript]

    plt.tight_layout()
    recipient_freq_chart_filename = f"{timestamp}_civic_recipient_address_frequency_bubble_chart.png"
    recipient_freq_chart_path = os.path.join(results_dir, recipient_freq_chart_filename)
    try:
        plt.savefig(recipient_freq_chart_path)
        print(f"Civic recipient address frequency bubble chart saved to {recipient_freq_chart_path}")
    except Exception as e:
        print(f"Error saving Civic recipient address frequency chart: {e}")
    plt.close()


# --- Common Save Function ---

def save_results_csv(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """Saves the raw results to a CSV file."""
    print("\nSaving raw results to CSV...")
    results_csv_filename = f"{timestamp}_analytics_results.csv"
    results_csv_path = os.path.join(results_dir, results_csv_filename)
    try:
        results_df = pd.DataFrame(results)
        results_df.to_csv(results_csv_path, index=False)
        print(f"Analytics results saved to {results_csv_path}")
    except Exception as e:
        print(f"Error saving results to CSV: {e}")


