import json
from collections import defaultdict


def calculate_hhi_for_ixps(file_path, country_code='US', metric='speed'):
    """
    Calculates the Herfindahl-Hirschman Index (HHI) for IXPs in a specific country.

    This function reads a PeeringDB JSON dump, identifies all IXPs in the specified
    country, and determines their market share based on the chosen metric.

    Args:
        file_path (str): The local path to the PeeringDB JSON dump file.
        country_code (str): The two-letter ISO 3166-1 alpha-2 country code.
        metric (str): The metric to use for market share ('speed' or 'asns').

    Returns:
        tuple: A tuple containing:
            - hhi_score (float): The calculated HHI for the country's IXP market.
            - ixp_details (list): A sorted list of tuples, where each tuple contains
                                  (IXP Name, Metric Value, Market Share).
               Returns None for both if the file or metric is invalid.
    """
    if metric not in ['speed', 'asns']:
        print(f"Error: Invalid metric '{metric}'. Choose 'speed' or 'asns'.")
        return None, None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None, None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from the file '{file_path}'.")
        return None, None

    # --- Step 1: Identify all IXPs in the target country ---
    target_ixps = {
        ix['id']: ix['name']
        for ix in data.get('ix', {}).get('data', [])
        if ix.get('country') == country_code
    }

    if not target_ixps:
        print(f"No IXPs found for country '{country_code}' in the dataset.")
        return 0.0, []

    # --- Step 2: Map IXP LANs back to their parent IXP ID ---
    ixlan_to_ix_map = {
        ixlan['id']: ixlan.get('ix_id')
        for ixlan in data.get('ixlan', {}).get('data', [])
        if ixlan.get('ix_id') in target_ixps
    }

    # --- Step 3: Aggregate market share metric for each target IXP ---
    ixp_market_values = defaultdict(lambda: defaultdict(int))  # To hold speed sums or sets of ASNs

    for netixlan in data.get('netixlan', {}).get('data', []):
        ixlan_id = netixlan.get('ixlan_id')
        if ixlan_id in ixlan_to_ix_map:
            ix_id = ixlan_to_ix_map[ixlan_id]
            if metric == 'speed':
                speed = netixlan.get('speed', 0)
                ixp_market_values[ix_id]['value'] += speed
            elif metric == 'asns':
                if 'value' not in ixp_market_values[ix_id]:
                    ixp_market_values[ix_id]['value'] = set()
                net_id = netixlan.get('net_id')
                ixp_market_values[ix_id]['value'].add(net_id)

    # Finalize the values (e.g., get length of sets for 'asns')
    final_ixp_values = {}
    for ix_id, data_dict in ixp_market_values.items():
        if metric == 'asns':
            final_ixp_values[ix_id] = len(data_dict['value'])
        else:
            final_ixp_values[ix_id] = data_dict['value']

    # --- Step 4: Calculate total market size ---
    total_market_size = sum(final_ixp_values.values())
    if total_market_size == 0:
        print(f"No market data found for metric '{metric}' in {country_code} IXPs.")
        return 0.0, []

    # --- Step 5: Calculate market share for each IXP ---
    ixp_details = []
    for ix_id, value in final_ixp_values.items():
        market_share = (value / total_market_size) * 100
        ixp_name = target_ixps.get(ix_id, f"Unknown IXP (ID: {ix_id})")

        display_value = value
        if metric == 'speed':
            # Convert Mbps to Gbps for display
            display_value /= 1000.0

        ixp_details.append((ixp_name, display_value, market_share))

    # --- Step 6: Calculate the HHI score ---
    hhi_score = sum(details[2] ** 2 for details in ixp_details)

    # Sort details for better presentation (by value, descending)
    ixp_details.sort(key=lambda x: x[1], reverse=True)

    return hhi_score, ixp_details


if __name__ == "__main__":
    # --- Configuration ---
    # Set the two-letter country code for the analysis.
    TARGET_COUNTRY = 'NL'  # Example: 'DE' for Germany, 'GB' for Great Britain

    # Choose the metric for market share: 'speed' or 'asns'
    METRIC_CHOICE = 'speed'

    # The path to your locally downloaded PeeringDB JSON file.
    # The data are provided by CAIDA at: https://www.caida.org/catalog/datasets/ixps/
    PEERINGDB_FILE = 'peeringdb_data/peeringdb_2_dump_2025_07_26.json'

    # --- Execution ---
    hhi, details = calculate_hhi_for_ixps(PEERINGDB_FILE, TARGET_COUNTRY, METRIC_CHOICE)

    if hhi is not None:
        # Determine display names based on metric
        if METRIC_CHOICE == 'speed':
            metric_display_name = "Port Capacity"
            header_name = "Capacity (Gbps)"
        else:  # asns
            metric_display_name = "Connected Networks"
            header_name = "Networks"

        print(f"--- IXP Market Concentration Analysis for {TARGET_COUNTRY} (by {metric_display_name}) ---")
        print(f"\nHerfindahl-Hirschman Index (HHI): {hhi:.2f}\n")

        # Interpretation of the HHI score
        if hhi < 1500:
            concentration_level = "Unconcentrated (Competitive Market)"
        elif 1500 <= hhi <= 2500:
            concentration_level = "Moderately Concentrated"
        else:
            concentration_level = "Highly Concentrated"
        print(f"Market Concentration Level: {concentration_level}")

        print(f"\n--- Top 15 IXPs in {TARGET_COUNTRY} by {metric_display_name} ---")
        print(f"{'IXP Name':<40} | {header_name:<17} | {'Market Share (%)':<20}")
        print("-" * 85)
        for name, value, share in details[:15]:
            if METRIC_CHOICE == 'speed':
                print(f"{name:<40} | {value:<17.1f} | {share:<20.2f}")
            else:
                print(f"{name:<40} | {int(value):<17} | {share:<20.2f}")
