#!/usr/bin/env python3
"""
PeeringDB ASN Network Type Extractor

This script queries the PeeringDB API to gather information about ASNs and their 
network types, then saves this data to both CSV and JSON formats.
"""

import csv
import json
import time
import requests
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Query PeeringDB for ASN network types")
    parser.add_argument("--output-dir", default="peeringdb_data", 
                        help="Directory to store output files")
    parser.add_argument("--format", choices=["csv", "json", "both"], default="both",
                        help="Output format (default: both)")
    parser.add_argument("--api-key", help="PeeringDB API key for authentication")
    parser.add_argument("--no-verify-ssl", action="store_true", 
                        help="Disable SSL certificate verification (not secure)")
    parser.add_argument("--max-retries", type=int, default=5,
                        help="Maximum number of retries for failed requests")
    return parser.parse_args()


def create_session(max_retries: int = 5, verify_ssl: bool = True) -> requests.Session:
    """
    Create a requests Session with retry configuration.
    
    Args:
        max_retries: Maximum number of retries for failed requests
        verify_ssl: Whether to verify SSL certificates
        
    Returns:
        Configured requests.Session object
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1,  # Wait 1, 2, 4, 8... seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Configure SSL verification
    session.verify = verify_ssl
    
    if not verify_ssl:
        logger.warning("SSL certificate verification disabled. This is not secure.")
    
    return session



def query_peeringdb_networks(api_key: Optional[str] = None, verify_ssl: bool = True, max_retries: int = 5) -> List[Dict[str, Any]]:
    """
    Query PeeringDB API for network data, handling pagination and retries.
    
    Args:
        api_key: Optional API key for authentication
        verify_ssl: Whether to verify SSL certificates
        max_retries: Maximum number of retries for failed requests
        
    Returns:
        List of network data dictionaries
    """
    base_url = "https://www.peeringdb.com/api/net"
    headers = {}
    
    if api_key:
        headers["Authorization"] = f"Api-Key {api_key}"
    
    # Create session with retry logic
    session = create_session(max_retries, verify_ssl)
    
    # Add consistent headers to all requests
    session.headers.update(headers)
    
    logger.info("Querying PeeringDB API for networks...")
    all_networks = []
    next_url = base_url
    
    while next_url:
        try:
            logger.debug(f"Fetching URL: {next_url}")
            response = session.get(next_url)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            data = response.json()
            networks = data.get("data", [])
            all_networks.extend(networks)
            
            logger.info(f"Retrieved {len(networks)} networks (total: {len(all_networks)})")
            
            # Check for pagination
            next_url = data.get("meta", {}).get("next")
            
            # Rate limiting - be gentle with the API even with retry logic
            if next_url:
                time.sleep(0.5)
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # Handle rate limiting manually (in addition to automatic retries)
                retry_after = int(e.response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                # Continue without incrementing page (will retry same URL)
                continue
            else:
                logger.error(f"HTTP error: {e}")
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            break
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON response")
            break
    
    return all_networks

def prepare_networks_for_save(networks: List[Dict[str, Any]], filepath: Path) -> List[Dict[str, str]]:
    """
    Prepare network data for saving by filtering and standardizing format.
    Also ensures the output directory exists.
    
    Args:
        networks: Raw list of network dictionaries
        filepath: Path where the output will be saved
        
    Returns:
        Filtered and standardized list of network dictionaries
    """
    # Filter out networks with empty network types
    filtered_networks = []
    
    for network in networks:
        if network.get('info_type'):
            filtered_networks.append({
                'asn': network.get('asn', ''),
                'network_type': network.get('info_type', ''),
                'network_name': network.get('name', '')
            })
    
    # Create output directory
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    return filtered_networks


def save_network_data(networks: List[Dict[str, Any]], filepath: Path, format_type: str) -> None:
    """
    Save network data to file in the specified format.
    
    Args:
        networks: List of network dictionaries
        filepath: Path object for the output file
        format_type: Type of format to save as ("csv" or "json")
    """
    # Common preprocessing
    output_data = prepare_networks_for_save(networks, filepath)
    
    if not output_data:
        logger.warning(f"No networks with defined types to save to {filepath}")
        return
    
    # Format-specific saving
    if format_type.lower() == "csv":
        with filepath.open('w', newline='', encoding='utf-8') as csvfile:
            fieldnames = list(output_data[0].keys())  # Use keys from first item
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_data)
    
    elif format_type.lower() == "json":
        with filepath.open('w', encoding='utf-8') as jsonfile:
            json.dump(output_data, jsonfile, indent=2)
    
    else:
        raise ValueError(f"Unsupported format type: {format_type}")
    
    logger.info(f"Saved {format_type.upper()} data to: {filepath} ({len(output_data)} networks with defined types)")


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
    
    logger.info("\nNetwork Type Distribution:")
    for net_type, count in sorted(type_count.items(), key=lambda x: x[1], reverse=True):
        # Special case only for NSP
        if net_type == "NSP":
            description = "Network Service Provider"
        else:
            description = net_type
        logger.info(f"  {description}: {count}")
    
    # Count networks without ASNs
    missing_asn = sum(1 for network in networks if not network.get('asn'))
    if missing_asn:
        logger.info(f"\nNetworks missing ASN information: {missing_asn}")
    
    # Count networks with empty type (now labeled as Unknown)
    empty_type_count = type_count.get('Unknown', 0)
    if empty_type_count:
        logger.info(f"\nASNs with empty network type (excluded from output files): {empty_type_count}")

def main():
    """Main function to extract and save network type data."""
    args = parse_arguments()
    
    # Create output directory - using pathlib
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Query PeeringDB API
    networks = query_peeringdb_networks(
        api_key=args.api_key, 
        verify_ssl=not args.no_verify_ssl,
        max_retries=args.max_retries
    )
    
    if not networks:
        logger.error("No data retrieved from PeeringDB. Exiting.")
        return
    
    logger.info(f"\nSuccessfully retrieved {len(networks)} networks from PeeringDB.")
    
    # Save to requested format(s)
    if args.format in ["csv", "both"]:
        csv_filepath = output_dir / f"asn_network_types_{timestamp}.csv"
        save_network_data(networks, csv_filepath, "csv")
    
    if args.format in ["json", "both"]:
        json_filepath = output_dir / f"asn_network_types_{timestamp}.json"
        save_network_data(networks, json_filepath, "json")
    
    # Analyze the data
    analyze_networks(networks)
    
    logger.info("\nTask completed successfully!")

    
if __name__ == "__main__":
    main()