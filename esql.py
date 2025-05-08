import argparse
import requests
import json
import sys
from dotenv import load_dotenv
import os
import pandas as pd
from rich.console import Console

def json_to_dataframe(data):
    columns = [col['name'] for col in data['columns']]
    values = data['values']
    df = pd.DataFrame(values, columns=columns)
    return df

def esql_query(elasticsearch_url, query, api_key):
    """
    Executes an ES|QL query against Elasticsearch using an API key.
    """
    endpoint = f"{elasticsearch_url}/_query"  # ES|QL endpoint
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {api_key}"
    }

    # Construct payload
    payload = {
        "query": query
    }

    try:
        # Make HTTP POST request
        response = requests.post(endpoint, headers=headers, json=payload)

        # Check for errors
        response.raise_for_status()

        # Parse and pretty-print JSON response
        output = response.json()
        return json_to_dataframe(output)
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - {response.text}")
        sys.exit(1)
    except Exception as err:
        print(f"An error occurred: {err}")
        sys.exit(1)

def main():
    """
    Command-line interface for ES|QL Elasticsearch queries.
    """
    load_dotenv()
    console = Console()

    # Load URL and API key from environment variables
    elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")

    if not elasticsearch_url:
        print("Error: Elasticsearch URL not found. Please set ELASTICSEARCH_URL in your .env file.")
        sys.exit(1)
    if not api_key:
        print("Error: API key not found. Please set ELASTICSEARCH_API_KEY in your .env file.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="CLI for executing ES|QL queries against Elasticsearch")
    parser.add_argument("--query", required=True, help="The ES|QL query string")

    args = parser.parse_args()

    # Run the ES|QL query
    result_df = esql_query(elasticsearch_url, args.query, api_key)

    # console.print(table)
    print(console.print(result_df))

if __name__ == "__main__":
    main()
