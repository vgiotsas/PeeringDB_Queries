
#!/usr/bin/env python3
"""
PeeringDB ASN Network Type Extractor

This script queries the PeeringDB API to gather information about ASNs and their 
network types, then saves this data to both CSV and JSON formats.
"""

import os
import csv
import json
import time
import requests
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Any

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Query PeeringDB for ASN network types")
    parser.add_argument("--output-dir", default="peeringdb_data", 
                        help="Directory to store output files")
    parser.add_argument("--format", choices=["csv", "json", "both"], default="both",
                        help="Output format (default: both)")
    parser.add_argument("--api-key", help="PeeringDB API key for authentication")
    return parser.parse_args()


def query_peeringdb_networks(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Query PeeringDB API for network data, handling pagination.
    
    Args:
        api_key: Optional API key for authentication
        
    Returns:
        List of network data dictionaries
    """
    base_url = "https://www.peeringdb.com/api/net"
    headers = {}
    
    if api_key:
        headers["Authorization"] = f"Api-Key {api_key}"
    
    print("Querying PeeringDB API for networks...")
    all_networks = []
    next_url = base_url
    
    while next_url:
        try:
            response = requests.get(next_url, headers=headers)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            data = response.json()
            networks = data.get("data", [])
            all_networks.extend(networks)
            
            print(f"Retrieved {len(networks)} networks (total: {len(all_networks)})")
            
            # Check for pagination
            next_url = data.get("meta", {}).get("next")
            
            # Respect rate limits
            if next_url:
                time.sleep(0.5)
                
        except requests.exceptions.RequestException as e:
            print(f"Error querying PeeringDB API: {e}")
            break
    
    return all_networks


def save_to_csv(networks: List[Dict[str, Any]], filename: str) -> None:
    """
    Save ASN and network type data to CSV file.
    
    Args:
        networks: List of network dictionaries
        filename: Output file path
    """
    # Filter out networks with empty network types
    filtered_networks = [n for n in networks if n.get('info_type')]
    
    # Create output directory
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['asn', 'network_type', 'network_name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for network in filtered_networks:
            writer.writerow({
                'asn': network.get('asn', ''),
                'network_type': network.get('info_type', ''),
                'network_name': network.get('name', '')
            })
    
    print(f"Saved CSV data to: {filename} ({len(filtered_networks)} networks with defined types)")


def save_to_json(networks: List[Dict[str, Any]], filename: str) -> None:
    """
    Save ASN and network type data to JSON file.
    
    Args:
        networks: List of network dictionaries
        filename: Output file path
    """
    # Filter out networks with empty network types
    filtered_networks = [n for n in networks if n.get('info_type')]
    
    # Create output directory
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    output_data = []
    
    for network in filtered_networks:
        output_data.append({
            'asn': network.get('asn', ''),
            'network_type': network.get('info_type', ''),
            'network_name': network.get('name', '')
        })
    
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(output_data, jsonfile, indent=2)
    
    print(f"Saved JSON data to: {filename} ({len(filtered_networks)} networks with defined types)")


def analyze_networks(networks: List[Dict[str, Any]]) -> None:
    """
    Print analysis of the retrieved network data.
    
    Args:
        networks: List of network dictionaries
    """
    # Count networks by type
    type_count = {}
    
    for network in networks:
        # Get network type, use 'Unknown' for empty strings
        net_type = network.get('info_type', '')
        if not net_type:
            net_type = 'Unknown'
            
        type_count[net_type] = type_count.get(net_type, 0) + 1
    
    print("\nNetwork Type Distribution:")
    for net_type, count in sorted(type_count.items(), key=lambda x: x[1], reverse=True):
        # Special case only for NSP
        if net_type == "NSP":
            description = "Network Service Provider"
        else:
            description = net_type
        print(f"  {description}: {count}")
    
    # Count networks without ASNs
    missing_asn = sum(1 for network in networks if not network.get('asn'))
    if missing_asn:
        print(f"\nNetworks missing ASN information: {missing_asn}")
    
    # Count networks with empty type (now labeled as Unknown)
    empty_type_count = type_count.get('Unknown', 0)
    if empty_type_count:
        print(f"\nASNs with empty network type (excluded from output files): {empty_type_count}")


def main():
    """Main function to extract and save network type data."""
    args = parse_arguments()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Query PeeringDB API
    networks = query_peeringdb_networks(args.api_key)
    
    if not networks:
        print("No data retrieved from PeeringDB. Exiting.")
        return
    
    print(f"\nSuccessfully retrieved {len(networks)} networks from PeeringDB.")
    
    os.makedirs(args.output_dir, exist_ok=True)

    # Generate filenames
    csv_filename = os.path.join(args.output_dir, f"asn_network_types_{timestamp}.csv")
    json_filename = os.path.join(args.output_dir, f"asn_network_types_{timestamp}.json")
    
    # Save to requested format(s)
    if args.format in ["csv", "both"]:
        save_to_csv(networks, csv_filename)
    
    if args.format in ["json", "both"]:
        save_to_json(networks, json_filename)
    
    # Analyze the data
    analyze_networks(networks)
    
    print("\nTask completed successfully!")

if __name__ == "__main__":
    main()