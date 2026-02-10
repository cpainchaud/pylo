"""
Integration tests for iplist_analyzer command's __main() function.

Tests full command execution with mocked Organization and validates
report generation for various scenarios.
"""
import csv
import json
import os
import sys
import tempfile
from pathlib import Path

# Add parent directory to path for test_fixtures import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
# Import command module
from illumio_pylo.cli.commands.iplist_analyzer import __main as iplist_analyzer_main
# Import test fixtures
from ..test_fixtures import MockOrganization, MockWorkload


# Reuse mocks from unit tests
class MockIP4Map:
    """Mock IP4Map for testing"""
    def __init__(self, ip_ranges=None, ip_count=0):
        self.ip_ranges = ip_ranges or []
        self._ip_count = ip_count
        self._subtracted_count = 0

    def count_ips(self):
        return self._ip_count - self._subtracted_count

    def to_string_list(self):
        return self.ip_ranges

    def substract(self, other_map):
        """Simulate IP subtraction - returns number of affected rows"""
        if not hasattr(other_map, 'ip_ranges'):
            return 0
        # Check if there's any overlap in IP ranges
        for my_range in self.ip_ranges:
            for other_range in other_map.ip_ranges:
                if my_range == other_range:
                    # Found overlap - subtract
                    self._subtracted_count += other_map._ip_count
                    return 1  # Affected rows
        return 0


class MockIPList(pylo.IPList):
    """Mock IPList for testing"""
    def __init__(self, name: str, href: str, entries: list, ip_map: MockIP4Map):
        mock_store = type('MockIPListStore', (), {'owner': None})()
        super().__init__(name=name, href=href, owner=mock_store)
        self._entries = entries
        self._ip_map = ip_map

    def get_raw_entries_as_string_list(self, separator="\n"):
        return separator.join(self._entries)

    def get_ip4map(self):
        # Return a fresh copy for proper testing
        return MockIP4Map(self._ip_map.ip_ranges, self._ip_map._ip_count)


class MockWorkloadForIPTest(MockWorkload):
    """Extended MockWorkload with IP4 map support"""
    def __init__(self, name: str, ip_map: MockIP4Map, appgroup: str = "App|Env|Loc", **kwargs):
        super().__init__(name=name, **kwargs)
        self._ip_map = ip_map
        self._appgroup = appgroup

    def get_ip4map_from_interfaces(self):
        return self._ip_map

    def get_appgroup_str(self):
        return self._appgroup


class MockOrganizationForIPTest(MockOrganization):
    """Extended MockOrganization with workload/iplist stores"""
    def __init__(self, workloads=None, iplists=None, **kwargs):
        super().__init__(**kwargs)
        self._workloads = workloads or []
        self._iplists = iplists or []

        # Setup IPList store
        self.IPListStore.items_by_href = {}
        for iplist in self._iplists:
            self.IPListStore.items_by_href[iplist.href] = iplist

        # Setup WorkloadStore
        mock_store = type('MockWorkloadStore', (), {
            'owner': self,
            'get_managed_workloads_list': lambda self: self.owner._workloads
        })()
        self.WorkloadStore = mock_store


def create_mock_org_with_test_data():
    """Create mock organization with test workloads and iplists"""
    # Create workloads with different IP ranges
    workload1 = MockWorkloadForIPTest(
        'web-prod-1',
        MockIP4Map(['192.168.1.0/24'], 256),
        appgroup="Web|Production|DC1"
    )
    workload2 = MockWorkloadForIPTest(
        'db-prod-1',
        MockIP4Map(['10.0.1.0/24'], 256),
        appgroup="DB|Production|DC1"
    )
    workload3 = MockWorkloadForIPTest(
        'web-dev-1',
        MockIP4Map(['172.16.1.0/24'], 256),
        appgroup="Web|Development|DC2"
    )

    # Create iplists
    iplist1 = MockIPList(
        'Private_Network',
        '/iplists/1',
        ['192.168.1.0/24', '10.0.1.0/24'],
        MockIP4Map(['192.168.1.0/24', '10.0.1.0/24'], 512)
    )
    iplist2 = MockIPList(
        'Dev_Network',
        '/iplists/2',
        ['172.16.1.0/24'],  # Match the workload exactly
        MockIP4Map(['172.16.1.0/24'], 256)
    )
    iplist3 = MockIPList(
        'Unused_Network',
        '/iplists/3',
        ['8.8.8.0/24'],
        MockIP4Map(['8.8.8.0/24'], 256)
    )

    org = MockOrganizationForIPTest(
        workloads=[workload1, workload2, workload3],
        iplists=[iplist1, iplist2, iplist3]
    )

    return org


# ============================================================================
# Test Functions
# ============================================================================

def test_main_basic_analysis():
    """Test __main() with basic IPList analysis"""
    print("=" * 60)
    print("Testing __main() - Basic Analysis")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_test_data()

        # Create test arguments
        args = {
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-basic.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with test data...")
        iplist_analyzer_main(args, org)

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-basic.csv'
        assert csv_file.exists(), "Expected CSV report to be created"
        print(f"[PASS] Report created: {csv_file}")

        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3, f"Expected 3 IPLists in report, got {len(rows)}"

            # Verify first iplist (Private_Network) - should match 2 workloads
            row1 = next(r for r in rows if r['name'] == 'Private_Network')
            assert row1['covered_workloads_count'] == '2', f"Expected 2 covered workloads for Private_Network"
            assert 'web-prod-1' in row1['covered_workloads_list'], "Expected web-prod-1 in covered list"
            assert 'db-prod-1' in row1['covered_workloads_list'], "Expected db-prod-1 in covered list"

            # Verify second iplist (Dev_Network) - should match 1 workload
            row2 = next(r for r in rows if r['name'] == 'Dev_Network')
            assert row2['covered_workloads_count'] == '1', f"Expected 1 covered workload for Dev_Network"
            assert 'web-dev-1' in row2['covered_workloads_list'], "Expected web-dev-1 in covered list"

            # Verify third iplist (Unused_Network) - should match no workloads
            row3 = next(r for r in rows if r['name'] == 'Unused_Network')
            assert row3['covered_workloads_count'] == '0', f"Expected 0 covered workloads for Unused_Network"
            assert row3['covered_workloads_list'] == '', "Expected empty covered list"

            print(f"[PASS] All {len(rows)} IPLists analyzed correctly")

    print("[PASS] Basic analysis test completed!\n")


def test_main_with_json_output():
    """Test __main() with JSON output format"""
    print("=" * 60)
    print("Testing __main() - JSON Output")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_test_data()

        # Create test arguments with JSON format
        args = {
            'report_format': ['json'],
            'output_dir': temp_dir,
            'output_filename': 'test-json.json'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with JSON output...")
        iplist_analyzer_main(args, org)

        # Verify JSON report generated
        json_file = Path(temp_dir) / 'test-json.json'
        assert json_file.exists(), "Expected JSON report to be created"

        # Read and verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            assert isinstance(data, list), "Expected JSON array"
            assert len(data) == 3, f"Expected 3 IPLists in JSON, got {len(data)}"

            # Verify structure
            for item in data:
                assert 'name' in item, "Expected 'name' field in JSON"
                assert 'ip4_count' in item, "Expected 'ip4_count' field in JSON"
                assert 'covered_workloads_count' in item, "Expected 'covered_workloads_count' field in JSON"

            print(f"[PASS] JSON report contains {len(data)} IPLists with correct structure")

    print("[PASS] JSON output test completed!\n")


def test_main_with_multiple_formats():
    """Test __main() with multiple output formats"""
    print("=" * 60)
    print("Testing __main() - Multiple Output Formats")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization
        org = create_mock_org_with_test_data()

        # Create test arguments with multiple formats
        args = {
            'report_format': ['csv', 'json', 'xlsx'],
            'output_dir': temp_dir,
            'output_filename': 'test-multi'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with CSV, JSON, and XLSX output...")
        iplist_analyzer_main(args, org)

        # Verify all reports generated
        csv_file = Path(temp_dir) / 'test-multi.csv'
        json_file = Path(temp_dir) / 'test-multi.json'
        xlsx_file = Path(temp_dir) / 'test-multi.xlsx'

        assert csv_file.exists(), "Expected CSV report to be created"
        assert json_file.exists(), "Expected JSON report to be created"
        assert xlsx_file.exists(), "Expected XLSX report to be created"
        print("[PASS] All three report formats created")

        # Verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_rows = list(csv.DictReader(f))
            assert len(csv_rows) == 3, f"Expected 3 rows in CSV"

        # Verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            assert len(json_data) == 3, f"Expected 3 items in JSON"

        print("[PASS] All formats contain correct data")

    print("[PASS] Multiple formats test completed!\n")


def test_main_with_no_workloads():
    """Test __main() with no workloads (empty org)"""
    print("=" * 60)
    print("Testing __main() - No Workloads")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create org with only iplists, no workloads
        iplist1 = MockIPList(
            'Test_Network',
            '/iplists/1',
            ['192.168.0.0/16'],
            MockIP4Map(['192.168.0.0/16'], 65536)
        )

        org = MockOrganizationForIPTest(
            workloads=[],  # No workloads
            iplists=[iplist1]
        )

        # Create test arguments
        args = {
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-no-workloads.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with no workloads...")
        iplist_analyzer_main(args, org)

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-no-workloads.csv'
        assert csv_file.exists(), "Expected CSV report to be created"

        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1, f"Expected 1 IPList in report, got {len(rows)}"

            # Verify no workloads covered
            row = rows[0]
            assert row['name'] == 'Test_Network', "Expected 'Test_Network'"
            assert row['covered_workloads_count'] == '0', "Expected 0 covered workloads"
            assert row['ip4_uncovered_count'] == row['ip4_count'], "Expected all IPs uncovered"

            print("[PASS] IPList with no workload coverage reported correctly")

    print("[PASS] No workloads test completed!\n")


def test_main_with_no_iplists():
    """Test __main() with no IPLists (empty analysis)"""
    print("=" * 60)
    print("Testing __main() - No IPLists")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create org with only workloads, no iplists
        workload1 = MockWorkloadForIPTest(
            'web-1',
            MockIP4Map(['192.168.1.0/24'], 256),
            appgroup="Web|Prod|DC1"
        )

        org = MockOrganizationForIPTest(
            workloads=[workload1],
            iplists=[]  # No iplists
        )

        # Create test arguments
        args = {
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-no-iplists.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with no IPLists...")
        iplist_analyzer_main(args, org)

        # Verify report generated (but empty)
        csv_file = Path(temp_dir) / 'test-no-iplists.csv'
        assert csv_file.exists(), "Expected CSV report to be created"

        # Read and verify CSV is empty
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 0, f"Expected 0 rows in report, got {len(rows)}"

            print("[PASS] Empty report generated correctly")

    print("[PASS] No IPLists test completed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all integration test functions"""
    print("\n" + "=" * 60)
    print("IPLIST ANALYZER INTEGRATION TESTS")
    print("=" * 60 + "\n")

    test_main_basic_analysis()
    test_main_with_json_output()
    test_main_with_multiple_formats()
    test_main_with_no_workloads()
    test_main_with_no_iplists()

    print("=" * 60)
    print("ALL INTEGRATION TESTS PASSED!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
