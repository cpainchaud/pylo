"""
Test suite for workload_export.py utility functions.

This test suite validates the refactored functions for testability,
including date formatting, workload row building, filter matching, and more.
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
from illumio_pylo.cli.commands.workload_export import (
    format_date_or_none,
    build_workload_row,
    match_filter_row_against_workload,
    build_report_headers,
    find_matching_filters_for_workload,
    ExtraColumnRegistry,
    ExtraColumn,
    FILTER_FIELD_HOSTNAME,
    FILTER_FIELD_APP,
    FILTER_FIELD_IP,
)

# Import shared test fixtures
from test_fixtures import (
    MockLabel,
    MockVENAgent,
    MockInterface,
    MockOrganization,
    MockWorkload,
)


class MockFilterData:
    """Mock filter data object that mimics CsvExcelToObject"""
    def __init__(self, rows):
        self._rows = rows
        self._detected_headers = list(rows[0].keys()) if rows else []

    def objects(self):
        return self._rows

    def count_columns(self):
        return len(self._detected_headers)

    def count_lines(self):
        return len(self._rows)


# ============================================================================
# Test Functions
# ============================================================================

def test_format_date_or_none():
    """Test date formatting function"""
    print("=" * 60)
    print("Testing format_date_or_none()")
    print("=" * 60)

    # Test with None
    result = format_date_or_none(None)
    assert result is None, "Should return None for None input"
    print("[PASS] None input returns None")

    # Test with valid datetime
    test_date = datetime(2024, 3, 15, 14, 30, 45)
    result = format_date_or_none(test_date)
    assert result == '2024-03-15 14:30:45', f"Expected '2024-03-15 14:30:45', got '{result}'"
    print("[PASS] Valid datetime formatted correctly")

    # Test with different date
    test_date2 = datetime(2023, 1, 1, 0, 0, 0)
    result2 = format_date_or_none(test_date2)
    assert result2 == '2023-01-01 00:00:00', f"Expected '2023-01-01 00:00:00', got '{result2}'"
    print("[PASS] Different datetime formatted correctly")

    print("\n[PASS] All format_date_or_none tests passed!\n")


def test_build_workload_row():
    """Test workload row building function"""
    print("=" * 60)
    print("Testing build_workload_row()")
    print("=" * 60)

    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])

    # Test basic workload without VEN agent
    workload = MockWorkload(
        name='test-server',
        hostname='test-server.example.com',
        online=True,
        unmanaged=False,
        interfaces=[MockInterface('192.168.1.100')],
        labels={
            'role': MockLabel('Web', 'role'),
            'app': MockLabel('MyApp', 'app'),
            'env': MockLabel('Production', 'env'),
        }
    )

    row = build_workload_row(workload, org)

    assert row['name'] == 'test-server', f"Expected name 'test-server', got '{row['name']}'"
    assert row['hostname'] == 'test-server.example.com', f"Expected hostname 'test-server.example.com', got '{row['hostname']}'"
    assert row['online'] is True, "Expected online to be True"
    assert row['managed'] is True, "Expected managed to be True"
    assert row['status'] == 'not-applicable', f"Expected status 'not-applicable' (no VEN agent), got '{row['status']}'"
    assert row['label_role'] == 'Web', f"Expected label_role 'Web', got '{row['label_role']}'"
    assert row['label_app'] == 'MyApp', f"Expected label_app 'MyApp', got '{row['label_app']}'"
    assert row['label_env'] == 'Production', f"Expected label_env 'Production', got '{row['label_env']}'"
    assert row['label_loc'] is None, "Expected label_loc to be None"
    print("[PASS] Basic workload row built correctly")

    # Test workload with VEN agent
    ven_agent = MockVENAgent(
        last_heartbeat=datetime(2024, 3, 15, 10, 30, 0),
        policy_applied_at=datetime(2024, 3, 15, 10, 25, 0),
        sync_state='synced',
        href='/agents/test-agent'
    )

    workload_with_agent = MockWorkload(
        name='server-with-agent',
        hostname='server-with-agent.example.com',
        ven_agent=ven_agent
    )

    row_with_agent = build_workload_row(workload_with_agent, org)

    assert row_with_agent['agent.href'] == '/agents/test-agent', "Expected agent.href to be set"
    assert row_with_agent['agent.sec_policy_sync_state'] == 'synced', "Expected sync state 'synced'"
    assert row_with_agent['agent.last_heartbeat'] == '2024-03-15 10:30:00', "Expected formatted heartbeat date"
    assert row_with_agent['agent.sec_policy_applied_at'] == '2024-03-15 10:25:00', "Expected formatted policy applied date"
    print("[PASS] Workload with VEN agent row built correctly")

    # Test unmanaged workload
    unmanaged_workload = MockWorkload(
        name='unmanaged-server',
        hostname='unmanaged-server.example.com',
        online=False,
        unmanaged=True
    )

    row_unmanaged = build_workload_row(unmanaged_workload, org)

    assert row_unmanaged['managed'] is False, "Expected managed to be False"
    assert row_unmanaged['online'] is False, "Expected online to be False"
    print("[PASS] Unmanaged workload row built correctly")

    print("\n[PASS] All build_workload_row tests passed!\n")


def test_match_filter_row_against_workload():
    """Test filter matching logic"""
    print("=" * 60)
    print("Testing match_filter_row_against_workload()")
    print("=" * 60)

    # Test hostname matching
    workload = MockWorkload(
        name='test-server',
        hostname='test-server.example.com',
        interfaces=[MockInterface('192.168.1.100')],
        labels={'app': MockLabel('MyApp', 'app')}
    )

    # Exact hostname match
    filter_row = {'hostname': 'test-server.example.com', '*line*': 1}
    result = match_filter_row_against_workload(workload, filter_row, [FILTER_FIELD_HOSTNAME])
    assert result is True, "Expected hostname to match"
    print("[PASS] Hostname exact match works")

    # Short hostname match (FQDN vs short)
    filter_row2 = {'hostname': 'test-server', '*line*': 2}
    result2 = match_filter_row_against_workload(workload, filter_row2, [FILTER_FIELD_HOSTNAME])
    assert result2 is True, "Expected short hostname to match FQDN"
    print("[PASS] Hostname short match works")

    # Hostname mismatch
    filter_row3 = {'hostname': 'other-server', '*line*': 3}
    result3 = match_filter_row_against_workload(workload, filter_row3, [FILTER_FIELD_HOSTNAME])
    assert result3 is False, "Expected hostname not to match"
    print("[PASS] Hostname mismatch works")

    # Test app label matching
    filter_row4 = {'app': 'MyApp', '*line*': 4}
    result4 = match_filter_row_against_workload(workload, filter_row4, [FILTER_FIELD_APP])
    assert result4 is True, "Expected app label to match"
    print("[PASS] App label match works")

    # App label case insensitive
    filter_row5 = {'app': 'myapp', '*line*': 5}
    result5 = match_filter_row_against_workload(workload, filter_row5, [FILTER_FIELD_APP])
    assert result5 is True, "Expected app label to match (case insensitive)"
    print("[PASS] App label case insensitive match works")

    # App label mismatch
    filter_row6 = {'app': 'OtherApp', '*line*': 6}
    result6 = match_filter_row_against_workload(workload, filter_row6, [FILTER_FIELD_APP])
    assert result6 is False, "Expected app label not to match"
    print("[PASS] App label mismatch works")

    # Test IP address matching
    filter_row7 = {'ip': '192.168.1.100', '*line*': 7}
    result7 = match_filter_row_against_workload(workload, filter_row7, [FILTER_FIELD_IP])
    assert result7 is True, "Expected IP to match"
    print("[PASS] IP address match works")

    # IP address mismatch
    filter_row8 = {'ip': '10.0.0.1', '*line*': 8}
    result8 = match_filter_row_against_workload(workload, filter_row8, [FILTER_FIELD_IP])
    assert result8 is False, "Expected IP not to match"
    print("[PASS] IP address mismatch works")

    # Multiple interfaces - one matches
    workload_multi_ip = MockWorkload(
        name='multi-ip-server',
        hostname='multi-ip.example.com',
        interfaces=[
            MockInterface('192.168.1.100'),
            MockInterface('10.0.0.50'),
            MockInterface('172.16.0.10')
        ]
    )
    filter_row9 = {'ip': '10.0.0.50', '*line*': 9}
    result9 = match_filter_row_against_workload(workload_multi_ip, filter_row9, [FILTER_FIELD_IP])
    assert result9 is True, "Expected IP to match one of multiple interfaces"
    print("[PASS] Multiple interfaces IP match works")

    # Test multiple filter fields (AND logic)
    filter_row10 = {'hostname': 'test-server', 'app': 'MyApp', '*line*': 10}
    result10 = match_filter_row_against_workload(workload, filter_row10, [FILTER_FIELD_HOSTNAME, FILTER_FIELD_APP])
    assert result10 is True, "Expected both hostname and app to match"
    print("[PASS] Multiple filter fields (both match) works")

    filter_row11 = {'hostname': 'test-server', 'app': 'OtherApp', '*line*': 11}
    result11 = match_filter_row_against_workload(workload, filter_row11, [FILTER_FIELD_HOSTNAME, FILTER_FIELD_APP])
    assert result11 is False, "Expected to fail when one field doesn't match"
    print("[PASS] Multiple filter fields (one fails) works")

    # Test empty/None filter values (should be ignored)
    filter_row12 = {'hostname': 'test-server', 'app': None, '*line*': 12}
    result12 = match_filter_row_against_workload(workload, filter_row12, [FILTER_FIELD_HOSTNAME, FILTER_FIELD_APP])
    assert result12 is True, "Expected None values to be ignored"
    print("[PASS] None filter values ignored correctly")

    # Test unsupported filter field
    filter_row13 = {'unsupported_field': 'value', '*line*': 13}
    try:
        match_filter_row_against_workload(workload, filter_row13, ['unsupported_field'])
        assert False, "Should have raised ValueError for unsupported field"
    except ValueError as e:
        assert 'not supported' in str(e), f"Expected 'not supported' in error message, got: {e}"
        print("[PASS] Unsupported filter field raises ValueError")

    print("\n[PASS] All match_filter_row_against_workload tests passed!\n")


def test_build_report_headers():
    """Test report header building"""
    print("=" * 60)
    print("Testing build_report_headers()")
    print("=" * 60)

    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])

    # Test without extra columns
    headers = build_report_headers(org, include_extra_columns=False)

    # ExcelHeaderSet is a list subclass, so iterate directly
    header_names = [h.name for h in headers]

    assert 'name' in header_names, "Expected 'name' in headers"
    assert 'hostname' in header_names, "Expected 'hostname' in headers"
    assert 'label_role' in header_names, "Expected 'label_role' in headers"
    assert 'label_app' in header_names, "Expected 'label_app' in headers"
    assert 'label_env' in header_names, "Expected 'label_env' in headers"
    assert 'label_loc' in header_names, "Expected 'label_loc' in headers"
    assert 'online' in header_names, "Expected 'online' in headers"
    assert 'managed' in header_names, "Expected 'managed' in headers"
    assert 'status' in header_names, "Expected 'status' in headers"
    assert 'agent.last_heartbeat' in header_names, "Expected 'agent.last_heartbeat' in headers"
    assert 'link_to_pce' in header_names, "Expected 'link_to_pce' in headers"
    assert 'href' in header_names, "Expected 'href' in headers"
    print("[PASS] Headers built correctly without extra columns")

    # Test with different label types
    org2 = MockOrganization(label_types=['custom1', 'custom2'])
    headers2 = build_report_headers(org2, include_extra_columns=False)
    header_names2 = [h.name for h in headers2]

    assert 'label_custom1' in header_names2, "Expected 'label_custom1' in headers"
    assert 'label_custom2' in header_names2, "Expected 'label_custom2' in headers"
    assert 'label_role' not in header_names2, "Expected 'label_role' not in headers"
    print("[PASS] Headers adapt to custom label types")

    print("\n[PASS] All build_report_headers tests passed!\n")


def test_find_matching_filters_for_workload():
    """Test finding all matching filters for a workload"""
    print("=" * 60)
    print("Testing find_matching_filters_for_workload()")
    print("=" * 60)

    workload = MockWorkload(
        name='test-server',
        hostname='test-server.example.com',
        interfaces=[MockInterface('192.168.1.100')],
        labels={'app': MockLabel('WebApp', 'app')}
    )
    workload = workload

    filter_data = MockFilterData([
        {'hostname': 'test-server', 'app': 'WebApp', '*line*': 1},
        {'hostname': 'other-server', 'app': 'WebApp', '*line*': 2},
        {'hostname': 'test-server', 'app': 'OtherApp', '*line*': 3},
        {'hostname': 'test-server', 'app': 'WebApp', '*line*': 4},  # Duplicate match
    ])

    # Test matching on hostname only
    matches = find_matching_filters_for_workload(workload, filter_data, [FILTER_FIELD_HOSTNAME])
    assert len(matches) == 3, f"Expected 3 matches (lines 1, 3, 4), got {len(matches)}"
    assert matches[0]['*line*'] == 1, "Expected first match to be line 1"
    assert matches[1]['*line*'] == 3, "Expected second match to be line 3"
    assert matches[2]['*line*'] == 4, "Expected third match to be line 4"
    print("[PASS] Finding filters by single field works")

    # Test matching on both hostname and app
    matches2 = find_matching_filters_for_workload(workload, filter_data, [FILTER_FIELD_HOSTNAME, FILTER_FIELD_APP])
    assert len(matches2) == 2, f"Expected 2 matches (lines 1, 4), got {len(matches2)}"
    assert matches2[0]['*line*'] == 1, "Expected first match to be line 1"
    assert matches2[1]['*line*'] == 4, "Expected second match to be line 4"
    print("[PASS] Finding filters by multiple fields works")

    # Test no matches
    no_match_workload = MockWorkload(
        name='nonexistent-server',
        hostname='nonexistent.example.com'
    )
    matches3 = find_matching_filters_for_workload(no_match_workload, filter_data, [FILTER_FIELD_HOSTNAME])
    assert len(matches3) == 0, f"Expected 0 matches, got {len(matches3)}"
    print("[PASS] No matches returns empty list")

    # Test error propagation for unsupported field
    filter_data_bad = MockFilterData([
        {'invalid_field': 'value', '*line*': 1}
    ])
    try:
        find_matching_filters_for_workload(workload, filter_data_bad, ['invalid_field'])
        assert False, "Should have raised PyloEx for unsupported field"
    except pylo.PyloEx as e:
        assert 'not supported' in str(e), f"Expected 'not supported' in error, got: {e}"
        print("[PASS] Error propagation works correctly")

    print("\n[PASS] All find_matching_filters_for_workload tests passed!\n")


def test_extra_column_registry():
    """Test ExtraColumnRegistry class"""
    print("=" * 60)
    print("Testing ExtraColumnRegistry")
    print("=" * 60)

    # Create a test registry
    registry = ExtraColumnRegistry()

    # Test empty registry
    assert len(registry.get_all()) == 0, "Expected empty registry initially"
    print("[PASS] Empty registry works")

    # Create mock extra columns
    class MockExtraColumn(ExtraColumn):
        def __init__(self, col_name: str):
            # Intentionally NOT calling super().__init__() to avoid global registration
            # noinspection PyMissingConstructor
            self.col_name = col_name

        def column_description(self):
            return ExtraColumn.ColumnDescription(self.col_name, self.col_name.title())

        def get_value(self, workload, org):
            return "test_value"

        def apply_cli_args(self, parser):
            pass

        def post_process_cli_args(self, args, org):
            pass

    col1 = MockExtraColumn("col1")
    col2 = MockExtraColumn("col2")

    # Test registration
    registry.register(col1)
    assert len(registry.get_all()) == 1, "Expected 1 column after registration"
    print("[PASS] Registration works")

    registry.register(col2)
    assert len(registry.get_all()) == 2, "Expected 2 columns after second registration"
    print("[PASS] Multiple registrations work")

    # Test get_all returns a copy
    cols = registry.get_all()
    cols.append(MockExtraColumn("col3"))
    assert len(registry.get_all()) == 2, "Expected get_all() to return a copy"
    print("[PASS] get_all() returns a copy")

    # Test get_column_descriptions
    descriptions = registry.get_column_descriptions()
    assert len(descriptions) == 2, "Expected 2 descriptions"
    assert descriptions[0].name == "col1", "Expected first description to be 'col1'"
    assert descriptions[1].name == "col2", "Expected second description to be 'col2'"
    print("[PASS] get_column_descriptions() works")

    # Test clear
    registry.clear()
    assert len(registry.get_all()) == 0, "Expected empty registry after clear"
    print("[PASS] Clear works")

    print("\n[PASS] All ExtraColumnRegistry tests passed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

if __name__ == '__main__':
    print("Workload Export Test Suite")
    print("=" * 60)

    try:
        test_format_date_or_none()
        test_build_workload_row()
        test_match_filter_row_against_workload()
        test_build_report_headers()
        test_find_matching_filters_for_workload()
        test_extra_column_registry()
        success = True
    except AssertionError as ae:
        print(f"\n[FAIL] Test failure: {ae}")
        success = False
    except Exception as e:
        print(f"\n[FAIL] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        success = False

    print("\n" + "=" * 60)
    if success:
        print("[PASS] All tests completed successfully!")
    else:
        print("[FAIL] Some tests failed!")
        sys.exit(1)
