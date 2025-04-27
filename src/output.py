import matplotlib.pyplot as plt
import pandas as pd
import os
from collections import Counter 
from typing import List, Dict, Any

def plot_decoding_success(successful_count: int, failed_count: int, results_dir: str, timestamp: int):
    """Generates and saves a bar chart for decoding success."""
    print("\nGenerating Decoding Success graphic...")

    labels = ['Successful Decodes', 'Failed Decodes']
    counts = [successful_count, failed_count]
    colors = ['#4CAF50', '#F44336']

    plt.figure(figsize=(8, 6))
    plt.bar(labels, counts, color=colors)
    plt.ylabel('Number of Transactions')
    plt.title('Transaction Input Data Decoding Results')
    plt.ylim(0, max(counts) * 1.1)

    for i, count in enumerate(counts):
        plt.text(i, count + (max(counts) * 0.02), str(count), ha='center')

    plt.tight_layout()
    decoding_chart_filename = f"{timestamp}_decoding_results_bar_chart.png"
    decoding_chart_path = os.path.join(results_dir, decoding_chart_filename)
    try:
        plt.savefig(decoding_chart_path)
        print(f"Decoding success bar chart saved to {decoding_chart_path}")
    except Exception as e:
        print(f"Error saving decoding success chart: {e}")
    plt.close()

def plot_genesis_transitions_over_time(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """Generates and saves a line chart for cumulative genesis transitions over time."""
    print("\nGenerating Genesis Transitions Over Time graphic...")

    genesis_transitions = [
        r for r in results
        if r.get("decoding_successful") and r.get("is_genesis_transition") and r.get("timestamp") is not None
    ]

    if not genesis_transitions:
        print("No genesis transition transactions with timestamps found to plot over time.")
        return

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
    genesis_chart_filename = f"{timestamp}_genesis_transitions_over_time.png"
    genesis_chart_path = os.path.join(results_dir, genesis_chart_filename)
    try:
        plt.savefig(genesis_chart_path)
        print(f"Genesis transitions chart saved to {genesis_chart_path}")
    except Exception as e:
        print(f"Error saving genesis transitions chart: {e}")
    plt.close()

def plot_identity_frequency_bubble_chart(results: List[Dict[str, Any]], results_dir: str, timestamp: int):
    """
    Generates and saves a bubble chart showing the frequency of identity IDs
    in 'transitState' transactions.
    """
    print("\nGenerating Identity Frequency graphic...")

    transit_state_ids = []
    for r in results:
        if (r.get("decoding_successful") and
            r.get("decoded_function") == "transitState" and
            r.get("decoded_parameters") and
            'id' in r["decoded_parameters"]):
            transit_state_ids.append(str(r["decoded_parameters"]["id"]))

    if not transit_state_ids:
        print("No 'transitState' transactions with identity IDs found to plot.")
        return

    id_counts = Counter(transit_state_ids)

    unique_ids = list(id_counts.keys())
    counts = list(id_counts.values())

    sorted_indices = sorted(range(len(counts)), key=lambda k: counts[k], reverse=True)
    sorted_unique_ids = [unique_ids[i] for i in sorted_indices]
    sorted_counts = [counts[i] for i in sorted_indices]

    x_indices = range(len(sorted_unique_ids))

    size_multiplier = 50
    bubble_sizes = [count * size_multiplier for count in sorted_counts]

    plt.figure(figsize=(15, 7)) 
    plt.scatter(
        x_indices,
        sorted_counts,
        s=bubble_sizes,
        alpha=0.6,
        edgecolors="w",
        linewidth=1
    )

    plt.xlabel('Unique Identity ID (Sorted by Frequency)')
    plt.ylabel('Number of Transactions')
    plt.title('Frequency of Identity IDs in Transit State Transactions')
    plt.grid(True, linestyle='--', alpha=0.6)

    for i in range(min(10, len(sorted_unique_ids))):
        plt.text(x_indices[i], sorted_counts[i], sorted_unique_ids[i][:8] + '...', fontsize=9, ha='center')

    num_ticks = min(20, len(sorted_unique_ids)) # Display up to 20 ticks
    tick_indices = [int(i * len(x_indices) / num_ticks) for i in range(num_ticks)]
    plt.xticks([x_indices[i] for i in tick_indices], [sorted_unique_ids[i][:10] + '...' for i in tick_indices], rotation=45, ha='right')


    plt.tight_layout()
    identity_freq_chart_filename = f"{timestamp}_identity_frequency_bubble_chart.png"
    identity_freq_chart_path = os.path.join(results_dir, identity_freq_chart_filename)
    try:
        plt.savefig(identity_freq_chart_path)
        print(f"Identity frequency bubble chart saved to {identity_freq_chart_path}")
    except Exception as e:
        print(f"Error saving identity frequency chart: {e}")
    plt.close()


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


