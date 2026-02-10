"""
Test suite for label_delete_unused.py utility functions.

Validates unused label detection, deletion limit logic, URL building,
and report generation.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from illumio_pylo.cli.commands.label_delete_unused import (
    find_unused_labels,
    apply_deletion_limit,
    build_pce_url,
    add_label_to_report,
)

# Import shared test fixtures
from test_fixtures import MockOrganization


class MockAPIConnector:
    """Mock API connector for testing"""
    def __init__(self, fqdn='pce.example.com', port=443, org_id=1):
        self.fqdn = fqdn
        self.port = port
        self.org_id = org_id


class MockSheet:
    """Mock Excel sheet for testing"""
    def __init__(self):
        self.rows = []

    def add_line_from_object(self, row):
        self.rows.append(row)


# ============================================================================
# Test Functions
# ============================================================================

def test_find_unused_labels():
    """Test finding unused labels from label JSON list"""
    print("=" * 60)
    print("Testing find_unused_labels()")
    print("=" * 60)

    # Test with no labels
    result = find_unused_labels([])
    assert result == [], "Expected empty list for no labels"
    print("[PASS] Empty list returns empty result")

    # Test with all used labels
    labels_json = [
        {'value': 'Production', 'key': 'env', 'href': '/labels/1', 'usage': {'workloads': True}},
        {'value': 'Web', 'key': 'role', 'href': '/labels/2', 'usage': {'workloads': True, 'rule_sets': True}},
    ]
    result = find_unused_labels(labels_json)
    assert len(result) == 0, f"Expected no unused labels, got {len(result)}"
    print("[PASS] All used labels filtered out")

    # Test with some unused labels
    labels_json = [
        {'value': 'Production', 'key': 'env', 'href': '/labels/1', 'usage': {'workloads': True}},
        {'value': 'Unused1', 'key': 'env', 'href': '/labels/2', 'usage': {'workloads': False, 'rule_sets': False}},
        {'value': 'Web', 'key': 'role', 'href': '/labels/3', 'usage': {'workloads': True}},
        {'value': 'Unused2', 'key': 'app', 'href': '/labels/4', 'usage': {}},
    ]
    result = find_unused_labels(labels_json)
    assert len(result) == 2, f"Expected 2 unused labels, got {len(result)}"
    assert result[0]['value'] == 'Unused1', f"Expected first unused label 'Unused1', got '{result[0]['value']}'"
    assert result[1]['value'] == 'Unused2', f"Expected second unused label 'Unused2', got '{result[1]['value']}'"
    print("[PASS] Unused labels correctly identified")

    # Test with all unused labels
    labels_json = [
        {'value': 'Unused1', 'key': 'env', 'href': '/labels/1', 'usage': {'workloads': False}},
        {'value': 'Unused2', 'key': 'role', 'href': '/labels/2', 'usage': {}},
    ]
    result = find_unused_labels(labels_json)
    assert len(result) == 2, f"Expected 2 unused labels, got {len(result)}"
    print("[PASS] All unused labels identified")

    # Test with missing usage field
    labels_json = [
        {'value': 'NoUsageField', 'key': 'env', 'href': '/labels/1'},
    ]
    result = find_unused_labels(labels_json)
    assert len(result) == 1, f"Expected 1 unused label (missing usage field), got {len(result)}"
    print("[PASS] Labels with missing usage field treated as unused")

    print("\n[PASS] All find_unused_labels tests passed!\n")


def test_apply_deletion_limit():
    """Test applying deletion limit to unused labels list"""
    print("=" * 60)
    print("Testing apply_deletion_limit()")
    print("=" * 60)

    # Test with no limit (None)
    unused_labels = [
        {'value': 'Label1', 'href': '/labels/1'},
        {'value': 'Label2', 'href': '/labels/2'},
        {'value': 'Label3', 'href': '/labels/3'},
    ]
    to_delete, ignored = apply_deletion_limit(unused_labels, None)
    assert len(to_delete) == 3, f"Expected 3 labels to delete, got {len(to_delete)}"
    assert len(ignored) == 0, f"Expected 0 ignored labels, got {len(ignored)}"
    print("[PASS] No limit returns all labels")

    # Test with limit less than total
    to_delete, ignored = apply_deletion_limit(unused_labels, 2)
    assert len(to_delete) == 2, f"Expected 2 labels to delete, got {len(to_delete)}"
    assert len(ignored) == 1, f"Expected 1 ignored label, got {len(ignored)}"
    assert to_delete[0]['value'] == 'Label1', f"Expected first label 'Label1', got '{to_delete[0]['value']}'"
    assert to_delete[1]['value'] == 'Label2', f"Expected second label 'Label2', got '{to_delete[1]['value']}'"
    assert ignored[0]['value'] == 'Label3', f"Expected ignored label 'Label3', got '{ignored[0]['value']}'"
    print("[PASS] Limit correctly splits labels")

    # Test with limit equal to total
    to_delete, ignored = apply_deletion_limit(unused_labels, 3)
    assert len(to_delete) == 3, f"Expected 3 labels to delete, got {len(to_delete)}"
    assert len(ignored) == 0, f"Expected 0 ignored labels, got {len(ignored)}"
    print("[PASS] Limit equal to total returns all labels")

    # Test with limit greater than total
    to_delete, ignored = apply_deletion_limit(unused_labels, 5)
    assert len(to_delete) == 3, f"Expected 3 labels to delete, got {len(to_delete)}"
    assert len(ignored) == 0, f"Expected 0 ignored labels, got {len(ignored)}"
    print("[PASS] Limit greater than total returns all labels")

    # Test with limit of 0
    to_delete, ignored = apply_deletion_limit(unused_labels, 0)
    assert len(to_delete) == 0, f"Expected 0 labels to delete, got {len(to_delete)}"
    assert len(ignored) == 3, f"Expected 3 ignored labels, got {len(ignored)}"
    print("[PASS] Limit of 0 ignores all labels")

    # Test with empty list
    to_delete, ignored = apply_deletion_limit([], 5)
    assert len(to_delete) == 0, f"Expected 0 labels to delete, got {len(to_delete)}"
    assert len(ignored) == 0, f"Expected 0 ignored labels, got {len(ignored)}"
    print("[PASS] Empty list returns empty results")

    print("\n[PASS] All apply_deletion_limit tests passed!\n")


def test_build_pce_url():
    """Test PCE URL building with different port configurations"""
    print("=" * 60)
    print("Testing build_pce_url()")
    print("=" * 60)

    # Test with default HTTPS port (443)
    connector = MockAPIConnector(fqdn='pce.example.com', port=443, org_id=1)
    url = build_pce_url(connector, '/sec_policy/draft/labels/123')
    expected = 'https://pce.example.com/orgs/1/sec_policy/draft/labels/123'
    assert url == expected, f"Expected '{expected}', got '{url}'"
    print(f"[PASS] Port 443 URL: {url}")

    # Test with custom port
    connector = MockAPIConnector(fqdn='pce.example.com', port=8443, org_id=1)
    url = build_pce_url(connector, '/sec_policy/draft/labels/456')
    expected = 'https://pce.example.com:8443/orgs/1/sec_policy/draft/labels/456'
    assert url == expected, f"Expected '{expected}', got '{url}'"
    print(f"[PASS] Custom port URL: {url}")

    # Test with different org ID
    connector = MockAPIConnector(fqdn='pce.company.com', port=443, org_id=99)
    url = build_pce_url(connector, '/sec_policy/draft/labels/789')
    expected = 'https://pce.company.com/orgs/99/sec_policy/draft/labels/789'
    assert url == expected, f"Expected '{expected}', got '{url}'"
    print(f"[PASS] Different org ID URL: {url}")

    # Test with empty href
    connector = MockAPIConnector(fqdn='pce.example.com', port=443, org_id=1)
    url = build_pce_url(connector, '')
    expected = 'https://pce.example.com/orgs/1'
    assert url == expected, f"Expected '{expected}', got '{url}'"
    print(f"[PASS] Empty href URL: {url}")

    print("\n[PASS] All build_pce_url tests passed!\n")


def test_add_label_to_report():
    """Test adding label to report sheet"""
    print("=" * 60)
    print("Testing add_label_to_report()")
    print("=" * 60)

    sheet = MockSheet()

    # Test basic label addition
    label_json = {
        'key': 'env',
        'value': 'Production',
        'href': '/sec_policy/draft/labels/1',
        'created_at': '2024-01-01T00:00:00Z',
        'updated_at': '2024-01-02T00:00:00Z',
        'external_data_set': 'external-set',
        'external_data_reference': 'external-ref',
        'usage': {'workloads': False, 'rule_sets': False}
    }
    pce_url = 'https://pce.example.com/orgs/1/sec_policy/draft/labels/1'

    add_label_to_report(label_json, sheet, pce_url, 'deleted')

    assert len(sheet.rows) == 1, f"Expected 1 row, got {len(sheet.rows)}"
    row = sheet.rows[0]
    assert row['key'] == 'env', f"Expected key 'env', got '{row['key']}'"
    assert row['value'] == 'Production', f"Expected value 'Production', got '{row['value']}'"
    assert row['type'] == 'env', f"Expected type 'env', got '{row['type']}'"
    assert row['action'] == 'deleted', f"Expected action 'deleted', got '{row['action']}'"
    assert row['error_message'] == '', f"Expected empty error message, got '{row['error_message']}'"
    assert row['link_to_pce'] == pce_url, f"Expected URL '{pce_url}', got '{row['link_to_pce']}'"
    assert row['usage_list'] == '', f"Expected empty usage list, got '{row['usage_list']}'"
    print("[PASS] Basic label added correctly")

    # Test label with usage
    sheet = MockSheet()
    label_json = {
        'key': 'role',
        'value': 'Web',
        'href': '/sec_policy/draft/labels/2',
        'created_at': '2024-01-01T00:00:00Z',
        'updated_at': '2024-01-02T00:00:00Z',
        'external_data_set': '',
        'external_data_reference': '',
        'usage': {'workloads': True, 'rule_sets': True, 'iplists': False}
    }
    pce_url = 'https://pce.example.com/orgs/1/sec_policy/draft/labels/2'

    add_label_to_report(label_json, sheet, pce_url, 'API error', 'Permission denied')

    assert len(sheet.rows) == 1, f"Expected 1 row, got {len(sheet.rows)}"
    row = sheet.rows[0]
    assert row['value'] == 'Web', f"Expected value 'Web', got '{row['value']}'"
    assert row['action'] == 'API error', f"Expected action 'API error', got '{row['action']}'"
    assert row['error_message'] == 'Permission denied', f"Expected error 'Permission denied', got '{row['error_message']}'"
    usage_list = row['usage_list']
    assert 'workloads' in usage_list, f"Expected 'workloads' in usage list, got '{usage_list}'"
    assert 'rule_sets' in usage_list, f"Expected 'rule_sets' in usage list, got '{usage_list}'"
    assert 'iplists' not in usage_list, f"Expected 'iplists' not in usage list, got '{usage_list}'"
    print(f"[PASS] Label with usage and error added correctly: {usage_list}")

    # Test label with missing fields
    sheet = MockSheet()
    label_json = {
        'key': 'app',
        'value': 'MyApp',
        'href': '/sec_policy/draft/labels/3',
    }
    pce_url = 'https://pce.example.com/orgs/1/sec_policy/draft/labels/3'

    add_label_to_report(label_json, sheet, pce_url, 'TO BE DELETED (no confirm option used)')

    assert len(sheet.rows) == 1, f"Expected 1 row, got {len(sheet.rows)}"
    row = sheet.rows[0]
    assert row['key'] == 'app', f"Expected key 'app', got '{row['key']}'"
    assert row['value'] == 'MyApp', f"Expected value 'MyApp', got '{row['value']}'"
    assert row['created_at'] == '', f"Expected empty created_at, got '{row['created_at']}'"
    assert row['updated_at'] == '', f"Expected empty updated_at, got '{row['updated_at']}'"
    assert row['external_data_set'] == '', f"Expected empty external_data_set, got '{row['external_data_set']}'"
    assert row['action'] == 'TO BE DELETED (no confirm option used)', f"Expected correct action"
    print("[PASS] Label with missing fields added correctly")

    print("\n[PASS] All add_label_to_report tests passed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all test functions"""
    print("\n" + "=" * 60)
    print("LABEL DELETE UNUSED UNIT TESTS")
    print("=" * 60 + "\n")

    test_find_unused_labels()
    test_apply_deletion_limit()
    test_build_pce_url()
    test_add_label_to_report()

    print("=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
