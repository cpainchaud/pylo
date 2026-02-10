"""
Test suite for iplist_analyzer.py utility functions.

Validates IP4 cache building, IPList coverage analysis, and report generation.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
from illumio_pylo.cli.commands.iplist_analyzer import (
    build_workloads_ip4_cache,
    build_iplists_ip4_cache,
    analyze_iplist_coverage,
    add_iplist_analysis_to_report,
)

# Import shared test fixtures
from test_fixtures import MockOrganization, MockWorkload, MockInterface


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

    def _setup_workload_store(self):
        """Setup WorkloadStore with get_managed_workloads_list method"""
        mock_store = type('MockWorkloadStore', (), {
            'owner': self,
            'get_managed_workloads_list': lambda self: self.owner._workloads
        })()
        self.WorkloadStore = mock_store


class MockSheet:
    """Mock Excel sheet for testing"""
    def __init__(self):
        self.rows = []

    def add_line_from_object(self, row):
        self.rows.append(row)

    def get_lines_count(self):
        return len(self.rows)


# ============================================================================
# Test Functions
# ============================================================================

def test_build_workloads_ip4_cache():
    """Test building workload IP4 cache"""
    print("=" * 60)
    print("Testing build_workloads_ip4_cache()")
    print("=" * 60)

    # Create mock workloads with IP maps
    workload1 = MockWorkloadForIPTest('web-1', MockIP4Map(['192.168.1.0/24'], 256))
    workload2 = MockWorkloadForIPTest('web-2', MockIP4Map(['10.0.0.0/24'], 256))

    org = MockOrganizationForIPTest(workloads=[workload1, workload2])
    org._setup_workload_store()

    # Build cache
    cache = build_workloads_ip4_cache(org)

    assert len(cache) == 2, f"Expected 2 workloads in cache, got {len(cache)}"
    assert workload1 in cache, "Expected workload1 in cache"
    assert workload2 in cache, "Expected workload2 in cache"
    assert cache[workload1].count_ips() == 256, "Expected 256 IPs for workload1"
    assert cache[workload2].count_ips() == 256, "Expected 256 IPs for workload2"
    print("[PASS] Workload IP4 cache built correctly")

    # Test with empty workload list
    org_empty = MockOrganizationForIPTest(workloads=[])
    org_empty._setup_workload_store()
    cache_empty = build_workloads_ip4_cache(org_empty)
    assert len(cache_empty) == 0, f"Expected empty cache, got {len(cache_empty)}"
    print("[PASS] Empty workload list handled correctly")

    print("\n[PASS] All build_workloads_ip4_cache tests passed!\n")


def test_build_iplists_ip4_cache():
    """Test building IPList IP4 cache"""
    print("=" * 60)
    print("Testing build_iplists_ip4_cache()")
    print("=" * 60)

    # Create mock iplists with IP maps
    iplist1 = MockIPList('Private_IPs', '/iplists/1', ['192.168.0.0/16'], MockIP4Map(['192.168.0.0/16'], 65536))
    iplist2 = MockIPList('Public_IPs', '/iplists/2', ['8.8.8.0/24'], MockIP4Map(['8.8.8.0/24'], 256))

    org = MockOrganizationForIPTest(iplists=[iplist1, iplist2])

    # Build cache
    cache = build_iplists_ip4_cache(org)

    assert len(cache) == 2, f"Expected 2 iplists in cache, got {len(cache)}"
    assert iplist1 in cache, "Expected iplist1 in cache"
    assert iplist2 in cache, "Expected iplist2 in cache"
    assert cache[iplist1].count_ips() == 65536, "Expected 65536 IPs for iplist1"
    assert cache[iplist2].count_ips() == 256, "Expected 256 IPs for iplist2"
    print("[PASS] IPList IP4 cache built correctly")

    # Test with empty iplist
    org_empty = MockOrganizationForIPTest(iplists=[])
    cache_empty = build_iplists_ip4_cache(org_empty)
    assert len(cache_empty) == 0, f"Expected empty cache, got {len(cache_empty)}"
    print("[PASS] Empty iplist handled correctly")

    print("\n[PASS] All build_iplists_ip4_cache tests passed!\n")


def test_analyze_iplist_coverage():
    """Test IPList coverage analysis"""
    print("=" * 60)
    print("Testing analyze_iplist_coverage()")
    print("=" * 60)

    # Create mock iplist
    iplist = MockIPList('Test_IPList', '/iplists/1', ['192.168.1.0/24'], MockIP4Map(['192.168.1.0/24'], 256))

    # Create workloads - one matching, one not matching
    workload1 = MockWorkloadForIPTest('web-1', MockIP4Map(['192.168.1.0/24'], 256), appgroup="Web|Prod|DC1")
    workload2 = MockWorkloadForIPTest('db-1', MockIP4Map(['10.0.0.0/24'], 256), appgroup="DB|Prod|DC1")

    workloads_cache = {
        workload1: workload1.get_ip4map_from_interfaces(),
        workload2: workload2.get_ip4map_from_interfaces()
    }

    # Analyze coverage
    result = analyze_iplist_coverage(iplist, workloads_cache)

    assert 'iplist' in result, "Expected 'iplist' in result"
    assert 'ip_map' in result, "Expected 'ip_map' in result"
    assert 'matched_workloads' in result, "Expected 'matched_workloads' in result"
    assert 'appgroup_tracker' in result, "Expected 'appgroup_tracker' in result"

    assert result['iplist'] == iplist, "Expected same iplist in result"
    assert len(result['matched_workloads']) == 1, f"Expected 1 matched workload, got {len(result['matched_workloads'])}"
    assert workload1 in result['matched_workloads'], "Expected workload1 to be matched"
    assert workload2 not in result['matched_workloads'], "Expected workload2 not to be matched"
    assert "Web|Prod|DC1" in result['appgroup_tracker'], "Expected app group in tracker"
    print("[PASS] IPList coverage analyzed correctly")

    # Test with no matching workloads
    iplist_nomatch = MockIPList('No_Match', '/iplists/2', ['172.16.0.0/16'], MockIP4Map(['172.16.0.0/16'], 65536))
    result_nomatch = analyze_iplist_coverage(iplist_nomatch, workloads_cache)

    assert len(result_nomatch['matched_workloads']) == 0, f"Expected no matches, got {len(result_nomatch['matched_workloads'])}"
    assert len(result_nomatch['appgroup_tracker']) == 0, f"Expected empty appgroup tracker, got {len(result_nomatch['appgroup_tracker'])}"
    print("[PASS] No matching workloads handled correctly")

    # Test with empty workload cache
    result_empty = analyze_iplist_coverage(iplist, {})
    assert len(result_empty['matched_workloads']) == 0, "Expected no matches with empty cache"
    print("[PASS] Empty workload cache handled correctly")

    print("\n[PASS] All analyze_iplist_coverage tests passed!\n")


def test_add_iplist_analysis_to_report():
    """Test adding IPList analysis to report"""
    print("=" * 60)
    print("Testing add_iplist_analysis_to_report()")
    print("=" * 60)

    sheet = MockSheet()

    # Create mock analysis result
    iplist = MockIPList('Test_IPList', '/iplists/1', ['192.168.1.0/24', '10.0.0.0/24'],
                        MockIP4Map(['192.168.1.0/24', '10.0.0.0/24'], 512))
    workload1 = MockWorkloadForIPTest('web-1', MockIP4Map(['192.168.1.0/24'], 256), appgroup="Web|Prod|DC1")
    workload2 = MockWorkloadForIPTest('web-2', MockIP4Map(['192.168.1.0/24'], 256), appgroup="Web|Prod|DC2")

    ip_map = iplist.get_ip4map()
    ip_map_after = MockIP4Map(['10.0.0.0/24'], 256)  # After subtraction

    analysis_result = {
        'iplist': iplist,
        'ip_map': ip_map,
        'ip_map_after_substraction': ip_map_after,
        'matched_workloads': [workload1, workload2],
        'appgroup_tracker': {'Web|Prod|DC1': True, 'Web|Prod|DC2': True}
    }

    # Add to report
    add_iplist_analysis_to_report(analysis_result, sheet)

    assert sheet.get_lines_count() == 1, f"Expected 1 row, got {sheet.get_lines_count()}"
    row = sheet.rows[0]

    assert row['name'] == 'Test_IPList', f"Expected name 'Test_IPList', got '{row['name']}'"
    assert row['href'] == '/iplists/1', f"Expected href '/iplists/1', got '{row['href']}'"
    assert row['ip4_count'] == 512, f"Expected ip4_count 512, got {row['ip4_count']}"
    assert row['ip4_uncovered_count'] == 256, f"Expected uncovered 256, got {row['ip4_uncovered_count']}"
    assert row['covered_workloads_count'] == 2, f"Expected 2 covered workloads, got {row['covered_workloads_count']}"
    assert 'web-1' in row['covered_workloads_list'], "Expected 'web-1' in workloads list"
    assert 'web-2' in row['covered_workloads_list'], "Expected 'web-2' in workloads list"
    assert 'Web|Prod|DC1' in row['covered_workloads_appgroups'], "Expected appgroup in list"
    print("[PASS] IPList analysis added to report correctly")

    # Test with no matched workloads
    sheet_empty = MockSheet()
    analysis_empty = {
        'iplist': iplist,
        'ip_map': ip_map,
        'ip_map_after_substraction': ip_map,
        'matched_workloads': [],
        'appgroup_tracker': {}
    }
    add_iplist_analysis_to_report(analysis_empty, sheet_empty)

    assert sheet_empty.get_lines_count() == 1, "Expected 1 row even with no matches"
    row_empty = sheet_empty.rows[0]
    assert row_empty['covered_workloads_count'] == 0, "Expected 0 covered workloads"
    assert row_empty['covered_workloads_list'] == '', "Expected empty workloads list"
    print("[PASS] Empty analysis result handled correctly")

    print("\n[PASS] All add_iplist_analysis_to_report tests passed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all test functions"""
    print("\n" + "=" * 60)
    print("IPLIST ANALYZER UNIT TESTS")
    print("=" * 60 + "\n")

    test_build_workloads_ip4_cache()
    test_build_iplists_ip4_cache()
    test_analyze_iplist_coverage()
    test_add_iplist_analysis_to_report()

    print("=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
