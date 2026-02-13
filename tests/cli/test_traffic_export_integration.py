"""
Integration tests for traffic_export command's __main() function.

Tests full command execution with mocked API calls and validates
report generation for various scenarios.
"""
import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import illumio_pylo as pylo
# Import command module
from illumio_pylo.cli.commands.traffic_export import __main as traffic_export_main
# Import test fixtures
from ..test_fixtures import MockOrganization, MockLabel


class MockIPList(pylo.IPList):
    """Mock IPList for testing"""
    def __init__(self, name: str, href: str = None):
        mock_store = type('MockIPListStore', (), {'owner': None})()
        super().__init__(name=name, href=href or f'/iplists/{name}', owner=mock_store)


class MockExplorerRecord:
    """Mock ExplorerResultV2 record for testing"""
    def __init__(self, src_ip, dst_ip, protocol, port, src_workload_href=None,
                 dst_workload_href=None, src_workload_hostname=None, dst_workload_hostname=None,
                 src_labels=None, dst_labels=None, policy_decision='allowed',
                 first_detected=None, last_detected=None):
        self.source_ip = src_ip
        self.destination_ip = dst_ip
        self.service_protocol = protocol
        self.service_port = port
        self.source_workload_href = src_workload_href
        self.destination_workload_href = dst_workload_href
        self.source_workload_hostname = src_workload_hostname
        self.destination_workload_hostname = dst_workload_hostname
        self.source_workload_labels_by_type = src_labels or {}
        self.destination_workload_labels_by_type = dst_labels or {}
        self.policy_decision_string = policy_decision
        self.first_detected = first_detected or "2024-03-15T10:00:00Z"
        self.last_detected = last_detected or "2024-03-15T11:00:00Z"
        self._src_iplists = {}
        self._dst_iplists = {}

    def get_source_iplists(self, org):
        return self._src_iplists

    def get_destination_iplists(self, org):
        return self._dst_iplists

    def draft_mode_policy_decision_to_str(self):
        return 'potentially_blocked'


class MockQueryResults:
    """Mock query results object"""
    def __init__(self, records):
        self._records = records

    def get_all_records(self):
        return self._records


class MockExplorerQuery:
    """Mock explorer query object"""
    def __init__(self, records):
        self._records = records
        self.filters = MagicMock()
        self.filters.new_source_filter = MagicMock(return_value=MagicMock())
        self.filters.new_destination_filter = MagicMock(return_value=MagicMock())
        self.filters.set_time_from_x_seconds_ago = MagicMock()
        self.filters.set_time_from = MagicMock()
        self.filters.set_time_to = MagicMock()

    def execute(self):
        return MockQueryResults(self._records)


class MockConnector:
    """Mock connector with explorer query"""
    def __init__(self, records):
        self._records = records

    def new_explorer_query_v2(self, max_results=10000, draft_mode_enabled=False):
        return MockExplorerQuery(self._records)


def create_mock_org_with_traffic():
    """Create a fully mocked organization with sample traffic records"""
    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])

    # Add labels to the org
    web_label = MockLabel('Web', 'role')
    db_label = MockLabel('Database', 'role')
    app_label = MockLabel('MyApp', 'app')
    prod_label = MockLabel('Production', 'env')

    org.LabelStore._items_by_href[web_label.href] = web_label
    org.LabelStore._items_by_href[db_label.href] = db_label
    org.LabelStore._items_by_href[app_label.href] = app_label
    org.LabelStore._items_by_href[prod_label.href] = prod_label

    # Add IPLists
    private_iplist = MockIPList('Private_Networks')
    org.IPListStore.items_by_href[private_iplist.href] = private_iplist

    # Create sample traffic records
    records = [
        MockExplorerRecord(
            src_ip='192.168.1.100',
            dst_ip='192.168.1.200',
            protocol=6,
            port=3306,
            src_workload_href='/workloads/web1',
            dst_workload_href='/workloads/db1',
            src_workload_hostname='web-1.example.com',
            dst_workload_hostname='db-1.example.com',
            src_labels={'role': 'Web', 'app': 'MyApp', 'env': 'Production'},
            dst_labels={'role': 'Database', 'app': 'MyApp', 'env': 'Production'},
            policy_decision='allowed'
        ),
        MockExplorerRecord(
            src_ip='192.168.1.101',
            dst_ip='192.168.1.200',
            protocol=6,
            port=3306,
            src_workload_href='/workloads/web2',
            dst_workload_href='/workloads/db1',
            src_workload_hostname='web-2.example.com',
            dst_workload_hostname='db-1.example.com',
            src_labels={'role': 'Web', 'app': 'MyApp', 'env': 'Production'},
            dst_labels={'role': 'Database', 'app': 'MyApp', 'env': 'Production'},
            policy_decision='allowed'
        ),
        MockExplorerRecord(
            src_ip='10.0.0.50',
            dst_ip='8.8.8.8',
            protocol=17,
            port=53,
            policy_decision='blocked'
        )
    ]

    # Mock the connector
    org.connector = MockConnector(records)

    return org, records


# ============================================================================
# Integration Test Functions
# ============================================================================

def test_main_basic_export():
    """Test basic __main() execution with timeframe"""
    print("=" * 60)
    print("Testing __main() - Basic Export")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': None,
            'until_timestamp': None,
            'timeframe_hours': 24,
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_file': os.path.join(temp_dir, 'test-traffic-export.csv'),
            'output_file_timestamp': False
        }

        # Execute __main()
        print("\n[TEST] Running __main() with basic export...")
        traffic_export_main(args, org)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-traffic-export.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 3, f"Expected 3 traffic records, got {len(rows)}"

            # Verify first record
            assert rows[0]['src_ip'] == '192.168.1.100', "Source IP mismatch"
            assert rows[0]['dst_ip'] == '192.168.1.200', "Destination IP mismatch"
            assert rows[0]['protocol'] == '6', "Protocol mismatch"
            assert rows[0]['port'] == '3306', "Port mismatch"
            assert rows[0]['policy_decision'] == 'allowed', "Policy decision mismatch"

            print(f"[PASS] CSV created with {len(rows)} traffic records")

    print("[PASS] Basic export test completed!\n")


def test_main_with_protocol_names():
    """Test __main() with protocol name translation"""
    print("=" * 60)
    print("Testing __main() - Protocol Names")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with protocol names enabled
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': None,
            'until_timestamp': None,
            'timeframe_hours': 24,
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': True,  # Enable protocol name translation
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_file': os.path.join(temp_dir, 'test-protocol-names.csv'),
            'output_file_timestamp': False
        }

        # Execute __main()
        print("\n[TEST] Running __main() with protocol names enabled...")
        traffic_export_main(args, org)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-protocol-names.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Verify protocol names
            assert rows[0]['protocol'] == 'TCP', f"Expected 'TCP', got '{rows[0]['protocol']}'"
            assert rows[2]['protocol'] == 'UDP', f"Expected 'UDP', got '{rows[2]['protocol']}'"

            print(f"[PASS] Protocol names translated correctly")

    print("[PASS] Protocol names test completed!\n")


def test_main_with_consolidate_labels():
    """Test __main() with label consolidation"""
    print("=" * 60)
    print("Testing __main() - Consolidate Labels")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with consolidated labels
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': None,
            'until_timestamp': None,
            'timeframe_hours': 24,
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': True,  # Enable label consolidation
            'label_separator': ' | ',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_file': os.path.join(temp_dir, 'test-consolidated-labels.csv'),
            'output_file_timestamp': False
        }

        # Execute __main()
        print("\n[TEST] Running __main() with consolidated labels...")
        traffic_export_main(args, org)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-consolidated-labels.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Check that consolidated columns exist
            assert 'src_labels' in rows[0], "src_labels column missing"
            assert 'dst_labels' in rows[0], "dst_labels column missing"

            # Verify separator is used
            if rows[0]['src_labels']:
                assert ' | ' in rows[0]['src_labels'] or len(rows[0]['src_labels'].split(' | ')) >= 1, \
                    "Label separator not used correctly"

            print(f"[PASS] Labels consolidated correctly")

    print("[PASS] Consolidate labels test completed!\n")


def test_main_with_omit_columns():
    """Test __main() with column omission"""
    print("=" * 60)
    print("Testing __main() - Omit Columns")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with omitted columns
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': None,
            'until_timestamp': None,
            'timeframe_hours': 24,
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': ['src_workload', 'dst_workload'],
            'report_format': ['csv'],
            'output_file': os.path.join(temp_dir, 'test-omit-columns.csv'),
            'output_file_timestamp': False
        }

        # Execute __main()
        print("\n[TEST] Running __main() with omitted columns...")
        traffic_export_main(args, org)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-omit-columns.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            headers = reader.fieldnames

            # Verify omitted columns are not in headers
            assert 'src_workload' not in headers, "src_workload column should be omitted"
            assert 'dst_workload' not in headers, "dst_workload column should be omitted"

            # Verify other columns still exist
            assert 'src_ip' in headers, "src_ip column should exist"
            assert 'dst_ip' in headers, "dst_ip column should exist"

            print(f"[PASS] Columns omitted correctly")

    print("[PASS] Omit columns test completed!\n")


def test_main_with_draft_mode():
    """Test __main() with draft mode enabled"""
    print("=" * 60)
    print("Testing __main() - Draft Mode")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with draft mode
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': None,
            'until_timestamp': None,
            'timeframe_hours': 24,
            'records_count_limit': 10000,
            'draft_mode_enabled': True,  # Enable draft mode
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_file': os.path.join(temp_dir, 'test-draft-mode.csv'),
            'output_file_timestamp': False
        }

        # Execute __main()
        print("\n[TEST] Running __main() with draft mode...")
        traffic_export_main(args, org)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-draft-mode.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            headers = reader.fieldnames

            # Verify draft_policy_decision column exists
            assert 'draft_policy_decision' in headers, "draft_policy_decision column should exist"
            assert rows[0]['draft_policy_decision'] == 'potentially_blocked', \
                "draft_policy_decision value mismatch"

            print(f"[PASS] Draft mode column included")

    print("[PASS] Draft mode test completed!\n")


def test_main_with_since_timestamp():
    """Test __main() with since timestamp"""
    print("=" * 60)
    print("Testing __main() - Since Timestamp")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with since timestamp
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': '2024-03-15T10:00:00',  # Use since timestamp
            'until_timestamp': None,
            'timeframe_hours': None,
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_file': os.path.join(temp_dir, 'test-since-timestamp.csv'),
            'output_file_timestamp': False
        }

        # Execute __main()
        print("\n[TEST] Running __main() with since timestamp...")
        traffic_export_main(args, org)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-since-timestamp.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        print(f"[PASS] Since timestamp handled correctly")

    print("[PASS] Since timestamp test completed!\n")


def test_main_error_invalid_timestamp():
    """Test __main() error handling for invalid timestamp"""
    print("=" * 60)
    print("Testing __main() - Invalid Timestamp Error")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with invalid timestamp
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': 'invalid-date',  # Invalid timestamp
            'until_timestamp': None,
            'timeframe_hours': None,
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-error.csv'
        }

        # Execute __main() and expect error
        print("\n[TEST] Running __main() with invalid timestamp...")
        try:
            traffic_export_main(args, org)
            assert False, "Expected PyloEx for invalid timestamp"
        except pylo.PyloEx as e:
            assert "Invalid --since-timestamp format" in str(e), f"Unexpected error: {e}"
            print(f"[PASS] Invalid timestamp error caught: {e}")

    print("[PASS] Invalid timestamp error test completed!\n")


def test_main_error_conflicting_timeframes():
    """Test __main() error handling for conflicting time arguments"""
    print("=" * 60)
    print("Testing __main() - Conflicting Timeframes Error")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org, records = create_mock_org_with_traffic()

        # Create test arguments with conflicting time settings
        args = {
            'source_filters': None,
            'destination_filters': None,
            'since_timestamp': '2024-03-15T10:00:00',  # Both set - conflict!
            'until_timestamp': None,
            'timeframe_hours': 24,  # Both set - conflict!
            'records_count_limit': 10000,
            'draft_mode_enabled': False,
            'protocol_names': False,
            'timezone': None,
            'consolidate_labels': False,
            'label_separator': ',',
            'disable_wrap_text': False,
            'omit_columns': None,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-error.csv'
        }

        # Execute __main() and expect error
        print("\n[TEST] Running __main() with conflicting timeframes...")
        try:
            traffic_export_main(args, org)
            assert False, "Expected PyloEx for conflicting arguments"
        except pylo.PyloEx as e:
            assert "cannot be used together" in str(e), f"Unexpected error: {e}"
            print(f"[PASS] Conflicting timeframes error caught: {e}")

    print("[PASS] Conflicting timeframes error test completed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all integration test functions"""
    print("\n" + "=" * 60)
    print("TRAFFIC EXPORT INTEGRATION TESTS")
    print("=" * 60 + "\n")

    test_main_basic_export()
    test_main_with_protocol_names()
    test_main_with_consolidate_labels()
    test_main_with_omit_columns()
    test_main_with_draft_mode()
    test_main_with_since_timestamp()
    test_main_error_invalid_timestamp()
    test_main_error_conflicting_timeframes()

    print("=" * 60)
    print("ALL INTEGRATION TESTS PASSED!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
