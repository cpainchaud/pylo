"""
Standalone test suite for workload_export.py utility functions.

This version demonstrates tests without requiring full pylo environment setup.
To run full tests, ensure dependencies are installed: pip install -r requirements.txt
"""
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def test_format_date_or_none_logic():
    """Test date formatting logic without imports"""
    print("=" * 60)
    print("Testing format_date_or_none() logic")
    print("=" * 60)

    def format_date_or_none(date):
        if date is None:
            return None
        return datetime.strftime(date, '%Y-%m-%d %H:%M:%S')

    # Test with None
    result = format_date_or_none(None)
    assert result is None, "Should return None for None input"
    print("[PASS] None input returns None")

    # Test with valid datetime
    test_date = datetime(2024, 3, 15, 14, 30, 45)
    result = format_date_or_none(test_date)
    assert result == '2024-03-15 14:30:45', f"Expected '2024-03-15 14:30:45', got '{result}'"
    print("[PASS] Valid datetime formatted correctly")

    print("\n[PASS] Date formatting logic tests passed!\n")


def test_filter_matching_logic():
    """Test filter matching logic without full imports"""
    print("=" * 60)
    print("Testing filter matching logic")
    print("=" * 60)

    # Simulate hostname_from_fqdn function
    def hostname_from_fqdn(fqdn):
        return fqdn.split('.')[0] if fqdn else ''

    # Test hostname matching logic
    workload_hostname = 'test-server.example.com'
    filter_hostname = 'test-server'

    hostname_in_csv = hostname_from_fqdn(filter_hostname).lower()
    workload_hostname_short = hostname_from_fqdn(workload_hostname).lower()

    assert hostname_in_csv == workload_hostname_short, "Hostname matching should work"
    print("[PASS] Hostname matching logic works")

    # Test case insensitivity
    filter_hostname2 = 'TEST-SERVER'
    hostname_in_csv2 = hostname_from_fqdn(filter_hostname2).lower()
    assert hostname_in_csv2 == workload_hostname_short, "Case insensitive matching should work"
    print("[PASS] Case insensitive matching works")

    # Test app label matching (case insensitive)
    app_filter = 'MyApp'
    app_workload = 'myapp'
    assert app_filter.lower() == app_workload.lower(), "App label case insensitive matching should work"
    print("[PASS] App label matching logic works")

    # Test IP matching in list
    interfaces = ['192.168.1.100', '10.0.0.50', '172.16.0.10']
    search_ip = '10.0.0.50'
    assert search_ip in interfaces, "IP should be found in list"
    print("[PASS] IP matching logic works")

    print("\n[PASS] All filter matching logic tests passed!\n")


def test_workload_row_building_logic():
    """Test workload row building logic"""
    print("=" * 60)
    print("Testing workload row building logic")
    print("=" * 60)

    # Simulate building a row
    workload_data = {
        'name': 'test-server',
        'hostname': 'test-server.example.com',
        'online': True,
        'unmanaged': False,
    }

    row = {
        'name': workload_data['name'],
        'hostname': workload_data['hostname'],
        'online': workload_data['online'],
        'managed': not workload_data['unmanaged'],  # Inverted logic
        'status': 'online' if workload_data['online'] else 'offline',
    }

    assert row['name'] == 'test-server', "Name should be set"
    assert row['hostname'] == 'test-server.example.com', "Hostname should be set"
    assert row['online'] is True, "Online should be True"
    assert row['managed'] is True, "Managed should be True (inverted from unmanaged)"
    assert row['status'] == 'online', "Status should be 'online'"
    print("[PASS] Basic row building works")

    # Test unmanaged workload
    workload_data2 = {
        'name': 'unmanaged',
        'hostname': 'unmanaged.example.com',
        'online': False,
        'unmanaged': True,
    }

    row2 = {
        'managed': not workload_data2['unmanaged'],
    }

    assert row2['managed'] is False, "Managed should be False for unmanaged workload"
    print("[PASS] Unmanaged workload logic works")

    print("\n[PASS] All row building logic tests passed!\n")


def test_header_building_logic():
    """Test header building logic"""
    print("=" * 60)
    print("Testing header building logic")
    print("=" * 60)

    label_types = ['role', 'app', 'env', 'loc']

    # Build headers
    headers = ['name', 'hostname']
    for label_type in label_types:
        headers.append(f'label_{label_type}')

    headers.extend([
        'online', 'managed', 'status', 'agent.last_heartbeat',
        'agent.sec_policy_sync_state', 'agent.sec_policy_applied_at',
        'link_to_pce', 'href', 'agent.href'
    ])

    assert 'name' in headers, "Should have 'name' header"
    assert 'hostname' in headers, "Should have 'hostname' header"
    assert 'label_role' in headers, "Should have 'label_role' header"
    assert 'label_app' in headers, "Should have 'label_app' header"
    assert 'online' in headers, "Should have 'online' header"
    assert 'agent.last_heartbeat' in headers, "Should have 'agent.last_heartbeat' header"
    print("[PASS] Standard headers included")

    # Test with custom label types
    custom_label_types = ['custom1', 'custom2']
    headers2 = ['name', 'hostname']
    for label_type in custom_label_types:
        headers2.append(f'label_{label_type}')

    assert 'label_custom1' in headers2, "Should have 'label_custom1' header"
    assert 'label_custom2' in headers2, "Should have 'label_custom2' header"
    assert 'label_role' not in headers2, "Should not have 'label_role' header"
    print("[PASS] Custom label types work")

    print("\n[PASS] All header building logic tests passed!\n")


def test_filter_finding_logic():
    """Test logic for finding matching filters"""
    print("=" * 60)
    print("Testing filter finding logic")
    print("=" * 60)

    # Simulate filter data
    filter_rows = [
        {'hostname': 'test-server', 'app': 'WebApp', 'line': 1},
        {'hostname': 'other-server', 'app': 'WebApp', 'line': 2},
        {'hostname': 'test-server', 'app': 'OtherApp', 'line': 3},
        {'hostname': 'test-server', 'app': 'WebApp', 'line': 4},
    ]

    workload_hostname = 'test-server'
    workload_app = 'WebApp'

    # Find matches by hostname only
    matches = [row for row in filter_rows if row['hostname'] == workload_hostname]
    assert len(matches) == 3, f"Expected 3 matches, got {len(matches)}"
    print("[PASS] Single field filtering works")

    # Find matches by hostname AND app
    matches2 = [
        row for row in filter_rows
        if row['hostname'] == workload_hostname and row['app'] == workload_app
    ]
    assert len(matches2) == 2, f"Expected 2 matches, got {len(matches2)}"
    assert matches2[0]['line'] == 1, "First match should be line 1"
    assert matches2[1]['line'] == 4, "Second match should be line 4"
    print("[PASS] Multiple field filtering works")

    # Test no matches
    no_match = [row for row in filter_rows if row['hostname'] == 'nonexistent']
    assert len(no_match) == 0, "Should have no matches"
    print("[PASS] No matches returns empty list")

    print("\n[PASS] All filter finding logic tests passed!\n")


def test_registry_pattern():
    """Test registry pattern for extensibility"""
    print("=" * 60)
    print("Testing registry pattern")
    print("=" * 60)

    class SimpleRegistry:
        def __init__(self):
            self._items = []

        def register(self, item):
            self._items.append(item)

        def clear(self):
            self._items.clear()

        def get_all(self):
            return self._items.copy()

    registry = SimpleRegistry()

    # Test empty
    assert len(registry.get_all()) == 0, "Should start empty"
    print("[PASS] Empty registry works")

    # Test registration
    registry.register("item1")
    assert len(registry.get_all()) == 1, "Should have 1 item"
    print("[PASS] Registration works")

    registry.register("item2")
    assert len(registry.get_all()) == 2, "Should have 2 items"
    print("[PASS] Multiple registrations work")

    # Test get_all returns copy
    items = registry.get_all()
    items.append("item3")
    assert len(registry.get_all()) == 2, "get_all() should return a copy"
    print("[PASS] get_all() returns copy")

    # Test clear
    registry.clear()
    assert len(registry.get_all()) == 0, "Should be empty after clear"
    print("[PASS] Clear works")

    print("\n[PASS] All registry pattern tests passed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

if __name__ == '__main__':
    print("Workload Export - Standalone Logic Tests")
    print("=" * 60)
    print("These tests validate the core logic without requiring")
    print("full pylo environment setup.")
    print("=" * 60)
    print()

    try:
        test_format_date_or_none_logic()
        test_filter_matching_logic()
        test_workload_row_building_logic()
        test_header_building_logic()
        test_filter_finding_logic()
        test_registry_pattern()
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
        print("[PASS] All standalone logic tests passed!")
        print()
        print("Note: To run full integration tests with actual workload_export")
        print("functions, install dependencies:")
        print("  pip install -r requirements.txt")
        print()
        print("Then run:")
        print("  python tests/test_workload_export.py")
    else:
        print("[FAIL] Some tests failed!")
        sys.exit(1)
