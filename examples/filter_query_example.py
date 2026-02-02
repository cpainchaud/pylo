"""
Example: Using the Filter Query feature to search workloads

This example demonstrates how to use the find_workloads_matching_query() method
to filter workloads using a SQL-like query syntax.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo


def main():
    # Load organization from PCE (using credentials file or cached data)
    # Replace 'your-pce-hostname' with your actual PCE hostname
    pce_hostname = 'your-pce-hostname'

    print(f"Loading PCE configuration from {pce_hostname}...")
    org = pylo.Organization(1)

    # Option 1: Load from API using saved credentials
    # org.load_from_api_using_credential_file(pce_hostname)

    # Option 2: Load from cache (if you've previously cached the data)
    try:
        org.load_from_cache_or_saved_credentials(pce_hostname)
    except Exception as e:
        print(f"Failed to load PCE data: {e}")
        print("\nTo use this example, you need to either:")
        print("  1. Configure credentials in ~/.pylo/credentials.json")
        print("  2. Have cached PCE data available")
        return

    print(f"Loaded {len(org.WorkloadStore.workloads)} workloads from PCE\n")

    # Example queries
    example_queries = [
        # Find workloads by name
        "name == 'web-server-01'",

        # Find workloads by partial name match
        "name contains 'web'",

        # Find workloads by name using regex
        "name matches 'web-.*-[0-9]+'",

        # Find workloads by IP address
        "ip_address == '192.168.1.100'",

        # Find online workloads
        "online == true",

        # Find workloads by label
        "env == 'Production'",
        "label.app == 'WebApp' and label.env == 'Production'",

        # Complex query with OR
        "(name == 'server-01' or name == 'server-02') and online == true",

        # Find workloads with old heartbeat
        "last_heartbeat <= '2024-01-01'",

        # Find workloads in a specific mode
        "mode == 'enforced'",

        # Find deleted workloads (need to pass include_deleted=True)
        "deleted == true",

        # Combine multiple conditions
        "hostname contains 'prod' and env == 'Production' and not deleted == true",
    ]

    print("=" * 60)
    print("Filter Query Examples")
    print("=" * 60)

    for query in example_queries:
        print(f"\nQuery: {query}")
        try:
            results = org.WorkloadStore.find_workloads_matching_query(query)
            print(f"Found {len(results)} workload(s)")
            for wkl in results[:5]:  # Show first 5 results
                print(f"  - {wkl.get_name()} ({wkl.href})")
            if len(results) > 5:
                print(f"  ... and {len(results) - 5} more")
        except pylo.PyloEx as e:
            print(f"Error: {e}")

    # Interactive query mode
    print("\n" + "=" * 60)
    print("Interactive Query Mode")
    print("Enter a query to search workloads, or 'quit' to exit")
    print("=" * 60)

    while True:
        try:
            query = input("\nQuery> ").strip()
            if query.lower() in ('quit', 'exit', 'q'):
                break
            if not query:
                continue

            results = org.WorkloadStore.find_workloads_matching_query(query)
            print(f"Found {len(results)} workload(s)")
            for wkl in results[:10]:
                labels = wkl.get_labels_str()
                print(f"  - {wkl.get_name()} | {labels} | online={wkl.online}")
            if len(results) > 10:
                print(f"  ... and {len(results) - 10} more")
        except pylo.PyloEx as e:
            print(f"Query error: {e}")
        except KeyboardInterrupt:
            break

    print("\nGoodbye!")


if __name__ == '__main__':
    main()
