"""
Integration tests for workload_export command's __main() function.

This test file simulates the full execution flow of the workload-export command
with mock PCE data, testing all major code paths including filter queries,
filter files, and report generation.
"""

import csv
import json as json_lib
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

# Import pylo and command module
from illumio_pylo.cli.commands.workload_export import __main as workload_export_main
# Import test fixtures
from test_fixtures import MockOrganization, MockWorkload, MockLabel, MockInterface, MockVENAgent


class MockWorkloadStore:
    """Mock WorkloadStore that simulates org.WorkloadStore"""
    def __init__(self, workloads):
        self.itemsByHRef = {w.href: w for w in workloads}
        self._workloads = workloads


def create_mock_org_with_workloads():
    """Create a fully mocked organization with sample workloads"""
    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])

    # Create sample workloads
    workloads = [
        MockWorkload(
            name='web-prod-1',
            hostname='web-prod-1.example.com',
            online=True,
            interfaces=[MockInterface('192.168.1.100')],
            labels={
                'role': MockLabel('Web', 'role'),
                'app': MockLabel('MyApp', 'app'),
                'env': MockLabel('Production', 'env'),
                'loc': MockLabel('US-East', 'loc')
            },
            ven_agent=MockVENAgent(sync_state='synced')
        ),
        MockWorkload(
            name='web-prod-2',
            hostname='web-prod-2.example.com',
            online=True,
            interfaces=[MockInterface('192.168.1.101')],
            labels={
                'role': MockLabel('Web', 'role'),
                'app': MockLabel('MyApp', 'app'),
                'env': MockLabel('Production', 'env'),
                'loc': MockLabel('US-East', 'loc')
            },
            ven_agent=MockVENAgent(sync_state='synced')
        ),
        MockWorkload(
            name='db-prod-1',
            hostname='db-prod-1.example.com',
            online=True,
            interfaces=[MockInterface('192.168.1.200')],
            labels={
                'role': MockLabel('Database', 'role'),
                'app': MockLabel('MyApp', 'app'),
                'env': MockLabel('Production', 'env'),
                'loc': MockLabel('US-West', 'loc')
            },
            ven_agent=MockVENAgent(sync_state='synced')
        ),
        MockWorkload(
            name='api-dev-1',
            hostname='api-dev-1.example.com',
            online=True,
            interfaces=[MockInterface('10.0.0.50')],
            labels={
                'role': MockLabel('API', 'role'),
                'app': MockLabel('MyApp', 'app'),
                'env': MockLabel('Development', 'env'),
                'loc': MockLabel('US-East', 'loc')
            },
            ven_agent=MockVENAgent(sync_state='synced')
        ),
        MockWorkload(
            name='test-server',
            hostname='test.example.com',
            online=False,
            unmanaged=True,
            interfaces=[MockInterface('10.0.0.99')]
        )
    ]

    # Mock the WorkloadStore
    org.WorkloadStore = MockWorkloadStore(workloads)

    return org


def test_main_basic_export():
    """Test basic __main() execution without filters"""
    print("=" * 60)
    print("Testing __main() - Basic Export")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_workloads()

        # Create test arguments
        args = {
            'verbose': False,
            'filter_query': None,
            'filter_file': None,
            'filter_file_delimiter': ',',
            'filter_fields': None,
            'keep_filters_in_report': False,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-basic-export.csv'
        }

        # Mock logger
        mock_logger = MagicMock()

        # Execute __main()
        print("\n[TEST] Running __main() with basic export...")
        workload_export_main(args, org, logger=mock_logger)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-basic-export.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 5, f"Expected 5 workloads, got {len(rows)}"

            # Verify first workload
            assert rows[0]['name'] == 'web-prod-1', "First workload name mismatch"
            assert rows[0]['hostname'] == 'web-prod-1.example.com', "Hostname mismatch"
            assert rows[0]['label_role'] == 'Web', "Role label mismatch"
            assert rows[0]['label_app'] == 'MyApp', "App label mismatch"
            assert rows[0]['label_env'] == 'Production', "Env label mismatch"

            print(f"[PASS] CSV created with {len(rows)} workloads")

    print("[PASS] Basic export test completed!\n")


def test_main_with_filter_query():
    """Test __main() with filter query"""
    print("=" * 60)
    print("Testing __main() - Filter Query")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_workloads()

        # Create test arguments with filter query
        args = {
            'verbose': False,
            'filter_query': "env == 'Production' and role == 'Web'",
            'filter_file': None,
            'filter_file_delimiter': ',',
            'filter_fields': None,
            'keep_filters_in_report': False,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-filtered-export.csv'
        }

        # Mock logger
        mock_logger = MagicMock()

        # Execute __main()
        print("\n[TEST] Running __main() with filter query...")
        workload_export_main(args, org, logger=mock_logger)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-filtered-export.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should only match web-prod-1 and web-prod-2
            assert len(rows) == 2, f"Expected 2 filtered workloads, got {len(rows)}"

            assert rows[0]['name'] == 'web-prod-1', "First filtered workload mismatch"
            assert rows[1]['name'] == 'web-prod-2', "Second filtered workload mismatch"

            # Verify both have correct labels
            for row in rows:
                assert row['label_role'] == 'Web', "Role should be Web"
                assert row['label_env'] == 'Production', "Env should be Production"

            print(f"[PASS] Filtered CSV created with {len(rows)} workloads")

    print("[PASS] Filter query test completed!\n")


def test_main_with_filter_file():
    """Test __main() with filter file"""
    print("=" * 60)
    print("Testing __main() - Filter File")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create filter CSV file
        filter_file_path = Path(temp_dir) / 'filter.csv'
        with open(filter_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['hostname', 'app'])
            writer.writerow(['web-prod-1', 'MyApp'])
            writer.writerow(['db-prod-1', 'MyApp'])

        # Create mock organization
        org = create_mock_org_with_workloads()

        # Create test arguments with filter file
        args = {
            'verbose': False,
            'filter_query': None,
            'filter_file': str(filter_file_path),
            'filter_file_delimiter': ',',
            'filter_fields': ['hostname', 'app'],
            'keep_filters_in_report': False,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-filter-file-export.csv'
        }

        # Mock logger
        mock_logger = MagicMock()

        # Execute __main()
        print("\n[TEST] Running __main() with filter file...")
        workload_export_main(args, org, logger=mock_logger)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-filter-file-export.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should match web-prod-1 and db-prod-1
            assert len(rows) == 2, f"Expected 2 filtered workloads, got {len(rows)}"

            names = [row['name'] for row in rows]
            assert 'web-prod-1' in names, "web-prod-1 should be in results"
            assert 'db-prod-1' in names, "db-prod-1 should be in results"

            print(f"[PASS] Filter file CSV created with {len(rows)} workloads")

    print("[PASS] Filter file test completed!\n")


def test_main_with_filter_file_keep_in_report():
    """Test __main() with filter file and keep_filters_in_report option"""
    print("=" * 60)
    print("Testing __main() - Filter File with Keep in Report")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create filter CSV file with extra column
        filter_file_path = Path(temp_dir) / 'filter.csv'
        with open(filter_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['hostname', 'app', 'owner'])
            writer.writerow(['web-prod-1', 'MyApp', 'TeamA'])
            writer.writerow(['db-prod-1', 'MyApp', 'TeamB'])
            writer.writerow(['nonexistent', 'MyApp', 'TeamC'])  # Won't match any workload

        # Create mock organization
        org = create_mock_org_with_workloads()

        # Create test arguments with keep_filters_in_report
        args = {
            'verbose': False,
            'filter_query': None,
            'filter_file': str(filter_file_path),
            'filter_file_delimiter': ',',
            'filter_fields': ['hostname', 'app'],
            'keep_filters_in_report': True,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-keep-filters-export.csv'
        }

        # Mock logger
        mock_logger = MagicMock()

        # Execute __main()
        print("\n[TEST] Running __main() with filter file and keep_filters_in_report...")
        workload_export_main(args, org, logger=mock_logger)

        # Verify CSV was created
        csv_file = Path(temp_dir) / 'test-keep-filters-export.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have 2 matched workloads + 1 unmatched filter = 3 rows
            assert len(rows) == 3, f"Expected 3 rows (2 matched + 1 unmatched filter), got {len(rows)}"

            # Check that filter columns exist
            assert '_hostname' in rows[0], "Filter hostname column missing"
            assert '_app' in rows[0], "Filter app column missing"
            assert '_owner' in rows[0], "Filter owner column missing"

            # Find matched workloads (have 'name' field populated)
            matched_rows = [r for r in rows if r['name']]
            unmatched_rows = [r for r in rows if not r['name']]

            assert len(matched_rows) == 2, f"Expected 2 matched workloads, got {len(matched_rows)}"
            assert len(unmatched_rows) == 1, f"Expected 1 unmatched filter, got {len(unmatched_rows)}"

            # Verify unmatched filter row
            unmatched = unmatched_rows[0]
            assert unmatched['_hostname'] == 'nonexistent', "Unmatched filter hostname incorrect"
            assert unmatched['_owner'] == 'TeamC', "Unmatched filter owner incorrect"

            print(f"[PASS] Keep filters CSV created with {len(matched_rows)} matched + {len(unmatched_rows)} unmatched")

    print("[PASS] Filter file with keep in report test completed!\n")


def test_main_multiple_formats():
    """Test __main() with multiple output formats"""
    print("=" * 60)
    print("Testing __main() - Multiple Formats")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_workloads()

        # Create test arguments for multiple formats
        args = {
            'verbose': False,
            'filter_query': None,
            'filter_file': None,
            'filter_file_delimiter': ',',
            'filter_fields': None,
            'keep_filters_in_report': False,
            'report_format': ['csv', 'xlsx', 'json'],
            'output_dir': temp_dir,
            'output_filename': 'test-multi-format'
        }

        # Mock logger
        mock_logger = MagicMock()

        # Execute __main()
        print("\n[TEST] Running __main() with multiple formats...")
        workload_export_main(args, org, logger=mock_logger)

        # Verify all format files were created
        csv_file = Path(temp_dir) / 'test-multi-format.csv'
        xlsx_file = Path(temp_dir) / 'test-multi-format.xlsx'
        json_file = Path(temp_dir) / 'test-multi-format.json'

        assert csv_file.exists(), f"CSV file not created: {csv_file}"
        assert xlsx_file.exists(), f"XLSX file not created: {xlsx_file}"
        assert json_file.exists(), f"JSON file not created: {json_file}"

        # Verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json_lib.load(f)
            assert isinstance(json_data, list), "JSON should be a list"
            assert len(json_data) == 5, f"Expected 5 workloads in JSON, got {len(json_data)}"

        print(f"[PASS] All 3 formats created successfully")

    print("[PASS] Multiple formats test completed!\n")


def test_main_empty_result():
    """Test __main() with filter that matches no workloads"""
    print("=" * 60)
    print("Testing __main() - Empty Result")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_workloads()

        # Create test arguments with filter that won't match anything
        args = {
            'verbose': False,
            'filter_query': "env == 'NonExistent'",
            'filter_file': None,
            'filter_file_delimiter': ',',
            'filter_fields': None,
            'keep_filters_in_report': False,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-empty-export.csv'
        }

        # Mock logger
        mock_logger = MagicMock()

        # Execute __main()
        print("\n[TEST] Running __main() with filter matching no workloads...")
        workload_export_main(args, org, logger=mock_logger)

        # Verify CSV was created (even if empty)
        csv_file = Path(temp_dir) / 'test-empty-export.csv'
        assert csv_file.exists(), f"CSV file not created: {csv_file}"

        # Read and validate CSV has headers but no data
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 0, f"Expected 0 workloads, got {len(rows)}"

            print(f"[PASS] Empty CSV created with headers only")

    print("[PASS] Empty result test completed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

if __name__ == '__main__':
    print("Workload Export Integration Test Suite")
    print("=" * 60)
    print()

    try:
        test_main_basic_export()
        test_main_with_filter_query()
        test_main_with_filter_file()
        test_main_with_filter_file_keep_in_report()
        test_main_multiple_formats()
        test_main_empty_result()
        success = True
    except AssertionError as ae:
        print(f"\n[FAIL] Test failure: {ae}")
        import traceback
        traceback.print_exc()
        success = False
    except Exception as e:
        print(f"\n[FAIL] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        success = False

    print("\n" + "=" * 60)
    if success:
        print("[PASS] All integration tests completed successfully!")
    else:
        print("[FAIL] Some tests failed!")
        sys.exit(1)
