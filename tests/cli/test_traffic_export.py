"""
Test suite for traffic_export.py utility functions.

Validates protocol conversion, timestamp handling, filter parsing,
column management, and record transformation logic.
"""
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
from illumio_pylo.cli.commands.traffic_export import (
    protocol_display,
    convert_timestamp,
    format_iplists,
    validate_time_arguments,
    build_column_list,
    parse_and_resolve_filter,
)

# Import shared test fixtures
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from test_fixtures import MockLabel, MockOrganization


class MockIPList(pylo.IPList):
    """Mock IPList for testing"""
    def __init__(self, name: str, href: str = None):
        mock_store = type('MockIPListStore', (), {'owner': None})()
        super().__init__(name=name, href=href or f'/iplists/{name}', owner=mock_store)


# ============================================================================
# Test Functions
# ============================================================================

def test_protocol_display():
    """Test protocol number to name translation"""
    print("=" * 60)
    print("Testing protocol_display()")
    print("=" * 60)

    # Test with enabled=True
    assert protocol_display(6, True) == 'TCP', "Expected 6 -> TCP"
    assert protocol_display(17, True) == 'UDP', "Expected 17 -> UDP"
    assert protocol_display(1, True) == 'ICMP', "Expected 1 -> ICMP"
    assert protocol_display(50, True) == 'ESP', "Expected 50 -> ESP"
    assert protocol_display(51, True) == 'AH', "Expected 51 -> AH"
    assert protocol_display(132, True) == 'SCTP', "Expected 132 -> SCTP"
    print("[PASS] Known protocols translated correctly")

    # Test with unknown protocol
    assert protocol_display(999, True) == 999, "Expected unknown protocol to return as-is"
    print("[PASS] Unknown protocol returned as-is")

    # Test with enabled=False
    assert protocol_display(6, False) == 6, "Expected no translation when disabled"
    assert protocol_display(17, False) == 17, "Expected no translation when disabled"
    print("[PASS] No translation when disabled")

    # Test with None
    assert protocol_display(None, True) is None, "Expected None to return None"
    print("[PASS] None input returns None")

    # Test with string input
    assert protocol_display('6', True) == 'TCP', "Expected string '6' -> TCP"
    assert protocol_display('invalid', True) == 'invalid', "Expected invalid string to return as-is"
    print("[PASS] String inputs handled correctly")

    print("\n[PASS] All protocol_display tests passed!\n")


def test_convert_timestamp():
    """Test timezone conversion for timestamps"""
    print("=" * 60)
    print("Testing convert_timestamp()")
    print("=" * 60)

    # Test with None inputs
    assert convert_timestamp(None, None) is None, "Expected None for None timestamp"
    assert convert_timestamp("2024-03-15T10:30:00Z", None) == "2024-03-15T10:30:00Z", "Expected no conversion without timezone"
    print("[PASS] None inputs handled correctly")

    # Test with timezone conversion (skip if tzdata not available on Windows)
    try:
        ny_tz = ZoneInfo("America/New_York")
        utc_time = "2024-03-15T15:00:00Z"  # 3 PM UTC
        result = convert_timestamp(utc_time, ny_tz)

        # Should be 10 AM or 11 AM EST/EDT depending on DST
        assert "2024-03-15" in result, f"Expected date 2024-03-15 in result: {result}"
        assert "T1" in result, f"Expected hour starting with 1 in result: {result}"
        print(f"[PASS] Timezone conversion works: {utc_time} -> {result}")

        # Test with invalid timestamp (should return original)
        invalid_time = "not-a-timestamp"
        result_invalid = convert_timestamp(invalid_time, ny_tz)
        assert result_invalid == invalid_time, "Expected invalid timestamp to return as-is"
        print("[PASS] Invalid timestamps returned as-is")
    except Exception as e:
        # tzdata not available on Windows - skip timezone tests
        if "tzdata" in str(e) or "ZoneInfoNotFoundError" in str(type(e).__name__):
            print("[SKIP] Timezone conversion tests (tzdata not available on this platform)")
        else:
            raise

    print("\n[PASS] All convert_timestamp tests passed!\n")


def test_format_iplists():
    """Test IPList dictionary formatting"""
    print("=" * 60)
    print("Testing format_iplists()")
    print("=" * 60)

    # Test with empty dict
    assert format_iplists({}) is None, "Expected None for empty dict"
    print("[PASS] Empty dict returns None")

    # Test with single IPList
    iplist1 = MockIPList("Private_Networks")
    result = format_iplists({'/iplists/1': iplist1})
    assert result == "Private_Networks", f"Expected 'Private_Networks', got '{result}'"
    print("[PASS] Single IPList formatted correctly")

    # Test with multiple IPLists (should be sorted case-insensitive)
    iplist2 = MockIPList("Public_IPs")
    iplist3 = MockIPList("allowed_networks")
    result = format_iplists({
        '/iplists/1': iplist1,
        '/iplists/2': iplist2,
        '/iplists/3': iplist3
    })
    assert "allowed_networks" in result, "Expected 'allowed_networks' in result"
    assert "Private_Networks" in result, "Expected 'Private_Networks' in result"
    assert "Public_IPs" in result, "Expected 'Public_IPs' in result"
    # Check alphabetical order (case-insensitive)
    parts = result.split(',')
    assert parts[0] == "allowed_networks", f"Expected 'allowed_networks' first, got '{parts[0]}'"
    print(f"[PASS] Multiple IPLists formatted and sorted: {result}")

    print("\n[PASS] All format_iplists tests passed!\n")


def test_validate_time_arguments():
    """Test time argument validation and conflict detection"""
    print("=" * 60)
    print("Testing validate_time_arguments()")
    print("=" * 60)

    # Test with timeframe_hours only
    since, until, seconds = validate_time_arguments(None, None, 24)
    assert since is None, "Expected since to be None"
    assert until is None, "Expected until to be None"
    assert seconds == 24 * 3600, f"Expected 86400 seconds, got {seconds}"
    print("[PASS] Timeframe hours converted correctly")

    # Test with since_timestamp only
    since, until, seconds = validate_time_arguments("2024-03-15T10:00:00", None, None)
    assert since == datetime(2024, 3, 15, 10, 0, 0), "Expected parsed datetime"
    assert until is None, "Expected until to be None"
    assert seconds is None, "Expected seconds to be None"
    print("[PASS] Since timestamp parsed correctly")

    # Test with both since and until
    since, until, seconds = validate_time_arguments("2024-03-15T10:00:00", "2024-03-15T12:00:00", None)
    assert since == datetime(2024, 3, 15, 10, 0, 0), "Expected parsed since datetime"
    assert until == datetime(2024, 3, 15, 12, 0, 0), "Expected parsed until datetime"
    assert seconds is None, "Expected seconds to be None"
    print("[PASS] Both since and until parsed correctly")

    # Test conflict detection (timeframe_hours with since/until)
    try:
        validate_time_arguments("2024-03-15T10:00:00", None, 24)
        assert False, "Expected PyloEx for conflicting arguments"
    except pylo.PyloEx as e:
        assert "cannot be used together" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Conflict detected: timeframe + since")

    # Test invalid timestamp format
    try:
        validate_time_arguments("not-a-date", None, None)
        assert False, "Expected PyloEx for invalid timestamp"
    except pylo.PyloEx as e:
        assert "Invalid --since-timestamp format" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Invalid timestamp format detected")

    # Test missing required arguments
    try:
        validate_time_arguments(None, None, None)
        assert False, "Expected PyloEx for missing arguments"
    except pylo.PyloEx as e:
        assert "Either --since-timestamp or --timeframe-hours must be provided" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Missing required arguments detected")

    print("\n[PASS] All validate_time_arguments tests passed!\n")


def test_build_column_list():
    """Test column list building with consolidation and omission"""
    print("=" * 60)
    print("Testing build_column_list()")
    print("=" * 60)

    label_types = ['role', 'app', 'env', 'loc']

    # Test without consolidation and without draft mode
    columns = build_column_list(label_types, False, False, None)
    assert 'src_role' in columns, "Expected src_role in columns"
    assert 'dst_app' in columns, "Expected dst_app in columns"
    assert 'protocol' in columns, "Expected protocol in columns"
    assert 'draft_policy_decision' not in columns, "Expected draft_policy_decision excluded"
    print("[PASS] Standard column list built correctly")

    # Test with consolidation
    columns = build_column_list(label_types, True, False, None)
    assert 'src_labels' in columns, "Expected src_labels in consolidated columns"
    assert 'dst_labels' in columns, "Expected dst_labels in consolidated columns"
    assert 'src_role' not in columns, "Expected individual label columns excluded"
    print("[PASS] Consolidated column list built correctly")

    # Test with draft mode enabled
    columns = build_column_list(label_types, False, True, None)
    assert 'draft_policy_decision' in columns, "Expected draft_policy_decision included"
    print("[PASS] Draft mode column included")

    # Test with omit columns
    columns = build_column_list(label_types, False, False, ['protocol', 'port'])
    assert 'protocol' not in columns, "Expected protocol omitted"
    assert 'port' not in columns, "Expected port omitted"
    assert 'src_ip' in columns, "Expected src_ip still included"
    print("[PASS] Columns omitted correctly")

    # Test invalid omit column
    try:
        build_column_list(label_types, False, False, ['invalid_column'])
        assert False, "Expected PyloEx for invalid column"
    except pylo.PyloEx as e:
        assert "Invalid column names" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Invalid column name detected")

    # Test omitting all columns
    all_cols = build_column_list(label_types, False, False, None)
    try:
        build_column_list(label_types, False, False, all_cols)
        assert False, "Expected PyloEx for omitting all columns"
    except pylo.PyloEx as e:
        assert "Cannot omit all columns" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Omitting all columns prevented")

    print("\n[PASS] All build_column_list tests passed!\n")


def test_parse_and_resolve_filter():
    """Test filter string parsing and resolution"""
    print("=" * 60)
    print("Testing parse_and_resolve_filter()")
    print("=" * 60)

    # Create mock organization with labels and iplists
    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])

    # Add mock label to store (must add to _items_by_href)
    web_label = MockLabel('Web', 'role')
    org.LabelStore._items_by_href[web_label.href] = web_label

    # Test label parsing
    filter_type, filter_obj = parse_and_resolve_filter('label:Web', org, 'source')
    assert filter_type == 'label', f"Expected filter_type 'label', got '{filter_type}'"
    assert filter_obj.name == 'Web', f"Expected label name 'Web', got '{filter_obj.name}'"
    print("[PASS] Label filter parsed correctly")

    # Add mock iplist to store (must add to items_by_href)
    private_iplist = MockIPList('Private_Networks')
    org.IPListStore.items_by_href[private_iplist.href] = private_iplist

    # Test iplist parsing
    filter_type, filter_obj = parse_and_resolve_filter('iplist:Private_Networks', org, 'source')
    assert filter_type == 'iplist', f"Expected filter_type 'iplist', got '{filter_type}'"
    assert filter_obj.name == 'Private_Networks', f"Expected iplist name 'Private_Networks', got '{filter_obj.name}'"
    print("[PASS] IPList filter parsed correctly")

    # Test invalid filter format
    try:
        parse_and_resolve_filter('invalid:filter', org, 'source')
        assert False, "Expected PyloEx for invalid filter format"
    except pylo.PyloEx as e:
        assert "Invalid source filter format" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Invalid filter format detected")

    # Test label not found
    try:
        parse_and_resolve_filter('label:NonExistent', org, 'destination')
        assert False, "Expected PyloEx for label not found"
    except pylo.PyloEx as e:
        assert "not found" in str(e), f"Unexpected error message: {e}"
        print("[PASS] Label not found detected")

    print("\n[PASS] All parse_and_resolve_filter tests passed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all test functions"""
    print("\n" + "=" * 60)
    print("TRAFFIC EXPORT UNIT TESTS")
    print("=" * 60 + "\n")

    test_protocol_display()
    test_convert_timestamp()
    test_format_iplists()
    test_validate_time_arguments()
    test_build_column_list()
    test_parse_and_resolve_filter()

    print("=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
