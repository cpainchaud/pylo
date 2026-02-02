#!/usr/bin/env python3
"""
Practical example of using environment variable credentials with Illumio Pylo
This demonstrates how to connect to a PCE using only environment variables
"""
import os
import illumio_pylo as pylo

def main():
    # Check if environment credentials are available
    from illumio_pylo.API.CredentialsManager import is_env_credentials_available

    if not is_env_credentials_available():
        print("ERROR: Required environment variables are not set!")
        print("Please set: PYLO_FQDN, PYLO_API_USER, PYLO_API_KEY")
        print("\nExample:")
        print("  export PYLO_FQDN='pce.example.com'")
        print("  export PYLO_API_USER='api_12345'")
        print("  export PYLO_API_KEY='your_api_key_here'")
        return 1

    print("Environment credentials detected!")
    print("-" * 60)

    # Load credentials from environment
    try:
        credentials = pylo.get_credentials_from_file('ENV')
        print(f"Credentials loaded:")
        print(f"  FQDN: {credentials.fqdn}")
        print(f"  Port: {credentials.port}")
        print(f"  Org ID: {credentials.org_id}")
        print(f"  API User: {credentials.api_user}")
        print(f"  Verify SSL: {credentials.verify_ssl}")
        print(f"  Source: {credentials.originating_file}")
        print("-" * 60)
    except pylo.PyloEx as e:
        print(f"ERROR loading credentials: {e}")
        return 1

    # Example 1: Create an APIConnector
    print("\nExample 1: Creating APIConnector from ENV profile")
    try:
        connector = pylo.APIConnector.create_from_credentials_in_file('ENV')
        print(f"✓ APIConnector created for {connector.fqdn}:{connector.port}")

        # Test connection by getting software version
        version = connector.get_software_version()
        print(f"✓ Connected successfully! PCE Version: {version}")

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("  (This is expected if the PCE is not accessible)")

    # Example 2: Load Organization data
    print("\nExample 2: Loading Organization from ENV profile")
    try:
        org = pylo.Organization.get_from_api_using_credential_file(
            'ENV',
            list_of_objects_to_load=[
                pylo.ObjectTypes.LABEL,
                pylo.ObjectTypes.WORKLOAD
            ]
        )
        print(f"✓ Organization loaded successfully!")
        print(f"  Labels: {len(org.LabelStore.itemsByHref)}")
        print(f"  Workloads: {len(org.WorkloadStore.itemsByHref)}")

    except Exception as e:
        print(f"✗ Failed to load organization: {e}")
        print("  (This is expected if the PCE is not accessible)")

    print("\n" + "=" * 60)
    print("Example complete!")
    return 0

if __name__ == '__main__':
    exit(main())
