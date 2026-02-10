"""
Integration tests for label_delete_unused command's __main() function.

Tests full command execution with mocked API calls and validates
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

# Import command module
from illumio_pylo.cli.commands.label_delete_unused import __main as label_delete_unused_main
# Import test fixtures
from ..test_fixtures import MockOrganization


class MockAPIConnector:
    """Mock API connector for testing"""
    def __init__(self, labels_json, fqdn='pce.example.com', port=443, org_id=1):
        self.labels_json = labels_json
        self.fqdn = fqdn
        self.port = port
        self.org_id = org_id
        self._deleted_labels = []
        self._deletion_errors = {}

    def objects_label_get(self, max_results=None, get_usage=False, async_mode=False):
        """Mock label fetching"""
        return self.labels_json

    def new_tracker_for_label_multi_deletion(self):
        """Create mock deletion tracker"""
        return MockDeletionTracker(self)


class MockDeletionTracker:
    """Mock deletion tracker for testing"""
    def __init__(self, connector):
        self.connector = connector
        self.labels_to_delete = []

    def add_label(self, href):
        """Add label to deletion list"""
        self.labels_to_delete.append(href)

    def execute_deletion(self):
        """Mock deletion execution"""
        for href in self.labels_to_delete:
            if href in self.connector._deletion_errors:
                # Error already configured for this label
                pass
            else:
                # Successful deletion
                self.connector._deleted_labels.append(href)

    def get_error(self, href):
        """Get error for specific label"""
        return self.connector._deletion_errors.get(href)

    def get_errors_count(self):
        """Get total error count"""
        return len(self.connector._deletion_errors)


def create_mock_connector_with_labels(labels_data, deletion_errors=None):
    """
    Create mock connector with label data.

    Args:
        labels_data: List of tuples (value, key, used)
        deletion_errors: Dict of href -> error message

    Returns:
        MockAPIConnector instance
    """
    labels_json = []
    for idx, (value, key, used) in enumerate(labels_data, start=1):
        label = {
            'value': value,
            'key': key,
            'href': f'/sec_policy/draft/labels/{idx}',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-02T00:00:00Z',
            'external_data_set': '',
            'external_data_reference': '',
            'usage': {'workloads': used, 'rule_sets': used} if used else {}
        }
        labels_json.append(label)

    connector = MockAPIConnector(labels_json)
    if deletion_errors:
        connector._deletion_errors = deletion_errors

    return connector


# ============================================================================
# Test Functions
# ============================================================================

def test_main_no_unused_labels():
    """Test __main() with no unused labels"""
    print("=" * 60)
    print("Testing __main() - No Unused Labels")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Production', 'env', True),
            ('Web', 'role', True),
            ('MyApp', 'app', True),
        ])

        # Create test arguments
        args = {
            'confirm': False,
            'limit': None,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-no-unused.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with all labels used...")
        label_delete_unused_main(args, org, connector)

        # Verify no deletions occurred
        assert len(connector._deleted_labels) == 0, f"Expected no deletions, got {len(connector._deleted_labels)}"
        print("[PASS] No labels deleted")

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-no-unused.csv'
        assert csv_file.exists(), "Expected CSV report to be created"
        print(f"[PASS] Report created: {csv_file}")

        # Read and verify CSV is empty (only headers)
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 0, f"Expected 0 data rows, got {len(rows)}"
            print("[PASS] Report contains no data rows")

    print("[PASS] No unused labels test completed!\n")


def test_main_unused_labels_without_confirm():
    """Test __main() with unused labels but no confirm flag"""
    print("=" * 60)
    print("Testing __main() - Unused Labels Without Confirm")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Production', 'env', True),
            ('UnusedEnv', 'env', False),
            ('Web', 'role', True),
            ('UnusedRole', 'role', False),
        ])

        # Create test arguments without confirm
        args = {
            'confirm': False,
            'limit': None,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-no-confirm.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() without confirm flag...")
        label_delete_unused_main(args, org, connector)

        # Verify no deletions occurred (dry-run mode)
        assert len(connector._deleted_labels) == 0, f"Expected no deletions in dry-run mode, got {len(connector._deleted_labels)}"
        print("[PASS] No labels deleted (dry-run mode)")

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-no-confirm.csv'
        assert csv_file.exists(), "Expected CSV report to be created"

        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2, f"Expected 2 data rows, got {len(rows)}"

            # Verify first row
            assert rows[0]['value'] == 'UnusedEnv', f"Expected 'UnusedEnv', got '{rows[0]['value']}'"
            assert rows[0]['type'] == 'env', f"Expected type 'env', got '{rows[0]['type']}'"
            assert rows[0]['action'] == 'TO BE DELETED (no confirm option used)', f"Unexpected action: {rows[0]['action']}"

            # Verify second row
            assert rows[1]['value'] == 'UnusedRole', f"Expected 'UnusedRole', got '{rows[1]['value']}'"
            assert rows[1]['type'] == 'role', f"Expected type 'role', got '{rows[1]['type']}'"
            assert rows[1]['action'] == 'TO BE DELETED (no confirm option used)', f"Unexpected action: {rows[1]['action']}"

            print(f"[PASS] Report contains {len(rows)} unused labels marked for deletion")

    print("[PASS] Unused labels without confirm test completed!\n")


def test_main_unused_labels_with_confirm():
    """Test __main() with unused labels and confirm flag"""
    print("=" * 60)
    print("Testing __main() - Unused Labels With Confirm")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Production', 'env', True),
            ('UnusedEnv1', 'env', False),
            ('UnusedEnv2', 'env', False),
            ('Web', 'role', True),
        ])

        # Create test arguments with confirm
        args = {
            'confirm': True,
            'limit': None,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-with-confirm.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with confirm flag...")
        label_delete_unused_main(args, org, connector)

        # Verify deletions occurred
        assert len(connector._deleted_labels) == 2, f"Expected 2 deletions, got {len(connector._deleted_labels)}"
        assert '/sec_policy/draft/labels/2' in connector._deleted_labels, "Expected label 2 to be deleted"
        assert '/sec_policy/draft/labels/3' in connector._deleted_labels, "Expected label 3 to be deleted"
        print(f"[PASS] {len(connector._deleted_labels)} labels deleted successfully")

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-with-confirm.csv'
        assert csv_file.exists(), "Expected CSV report to be created"

        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2, f"Expected 2 data rows, got {len(rows)}"

            # Verify all rows have 'deleted' action
            for row in rows:
                assert row['action'] == 'deleted', f"Expected action 'deleted', got '{row['action']}'"
                assert row['error_message'] == '', f"Expected no error, got '{row['error_message']}'"

            print(f"[PASS] Report shows {len(rows)} labels as deleted")

    print("[PASS] Unused labels with confirm test completed!\n")


def test_main_with_limit():
    """Test __main() with deletion limit"""
    print("=" * 60)
    print("Testing __main() - With Deletion Limit")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Unused1', 'env', False),
            ('Unused2', 'env', False),
            ('Unused3', 'env', False),
            ('Unused4', 'env', False),
            ('Unused5', 'env', False),
        ])

        # Create test arguments with limit
        args = {
            'confirm': True,
            'limit': 2,  # Only delete 2 labels
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-with-limit.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with limit=2...")
        label_delete_unused_main(args, org, connector)

        # Verify only 2 deletions occurred
        assert len(connector._deleted_labels) == 2, f"Expected 2 deletions, got {len(connector._deleted_labels)}"
        assert '/sec_policy/draft/labels/1' in connector._deleted_labels, "Expected label 1 to be deleted"
        assert '/sec_policy/draft/labels/2' in connector._deleted_labels, "Expected label 2 to be deleted"
        print(f"[PASS] Only {len(connector._deleted_labels)} labels deleted (respecting limit)")

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-with-limit.csv'
        assert csv_file.exists(), "Expected CSV report to be created"

        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 5, f"Expected 5 data rows (2 deleted + 3 ignored), got {len(rows)}"

            # Verify deleted labels
            deleted_rows = [r for r in rows if r['action'] == 'deleted']
            assert len(deleted_rows) == 2, f"Expected 2 deleted rows, got {len(deleted_rows)}"

            # Verify ignored labels
            ignored_rows = [r for r in rows if r['action'] == 'ignored (limit reached)']
            assert len(ignored_rows) == 3, f"Expected 3 ignored rows, got {len(ignored_rows)}"

            print(f"[PASS] Report shows {len(deleted_rows)} deleted and {len(ignored_rows)} ignored labels")

    print("[PASS] Deletion limit test completed!\n")


def test_main_with_deletion_errors():
    """Test __main() with API deletion errors"""
    print("=" * 60)
    print("Testing __main() - With Deletion Errors")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector with errors configured
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Unused1', 'env', False),
            ('Unused2', 'env', False),
            ('Unused3', 'env', False),
        ], deletion_errors={
            '/sec_policy/draft/labels/2': 'Permission denied',
            '/sec_policy/draft/labels/3': 'Label is locked',
        })

        # Create test arguments with confirm
        args = {
            'confirm': True,
            'limit': None,
            'report_format': ['csv'],
            'output_dir': temp_dir,
            'output_filename': 'test-with-errors.csv'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with deletion errors...")
        label_delete_unused_main(args, org, connector)

        # Verify partial deletion (only 1 succeeded)
        assert len(connector._deleted_labels) == 1, f"Expected 1 successful deletion, got {len(connector._deleted_labels)}"
        assert '/sec_policy/draft/labels/1' in connector._deleted_labels, "Expected label 1 to be deleted"
        print(f"[PASS] {len(connector._deleted_labels)} label deleted successfully, 2 failed")

        # Verify report generated
        csv_file = Path(temp_dir) / 'test-with-errors.csv'
        assert csv_file.exists(), "Expected CSV report to be created"

        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3, f"Expected 3 data rows, got {len(rows)}"

            # Verify deleted label
            deleted_rows = [r for r in rows if r['action'] == 'deleted']
            assert len(deleted_rows) == 1, f"Expected 1 deleted row, got {len(deleted_rows)}"
            assert deleted_rows[0]['value'] == 'Unused1', f"Expected 'Unused1' to be deleted"

            # Verify error labels
            error_rows = [r for r in rows if r['action'] == 'API error']
            assert len(error_rows) == 2, f"Expected 2 error rows, got {len(error_rows)}"

            # Check error messages
            error_messages = {r['value']: r['error_message'] for r in error_rows}
            assert error_messages['Unused2'] == 'Permission denied', f"Wrong error for Unused2"
            assert error_messages['Unused3'] == 'Label is locked', f"Wrong error for Unused3"

            print(f"[PASS] Report shows 1 success and 2 errors with correct messages")

    print("[PASS] Deletion errors test completed!\n")


def test_main_with_json_output():
    """Test __main() with JSON output format"""
    print("=" * 60)
    print("Testing __main() - With JSON Output")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Production', 'env', True),
            ('UnusedEnv', 'env', False),
        ])

        # Create test arguments with JSON format
        args = {
            'confirm': False,
            'limit': None,
            'report_format': ['json'],
            'output_dir': temp_dir,
            'output_filename': 'test-json-output.json'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with JSON output format...")
        label_delete_unused_main(args, org, connector)

        # Verify JSON report generated
        json_file = Path(temp_dir) / 'test-json-output.json'
        assert json_file.exists(), "Expected JSON report to be created"

        # Read and verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            assert isinstance(data, list), "Expected JSON array"
            assert len(data) == 1, f"Expected 1 data item, got {len(data)}"
            assert data[0]['value'] == 'UnusedEnv', f"Expected 'UnusedEnv', got '{data[0]['value']}'"
            assert data[0]['action'] == 'TO BE DELETED (no confirm option used)', f"Unexpected action"

            print(f"[PASS] JSON report contains {len(data)} label")

    print("[PASS] JSON output test completed!\n")


def test_main_with_multiple_formats():
    """Test __main() with multiple output formats"""
    print("=" * 60)
    print("Testing __main() - With Multiple Output Formats")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock organization and connector
        org = MockOrganization()
        connector = create_mock_connector_with_labels([
            ('Unused1', 'env', False),
            ('Unused2', 'role', False),
        ])

        # Create test arguments with multiple formats
        args = {
            'confirm': True,
            'limit': None,
            'report_format': ['csv', 'json'],
            'output_dir': temp_dir,
            'output_filename': 'test-multi-format'
        }

        # Execute __main()
        print("\n[TEST] Running __main() with CSV and JSON output...")
        label_delete_unused_main(args, org, connector)

        # Verify both reports generated
        csv_file = Path(temp_dir) / 'test-multi-format.csv'
        json_file = Path(temp_dir) / 'test-multi-format.json'

        assert csv_file.exists(), "Expected CSV report to be created"
        assert json_file.exists(), "Expected JSON report to be created"
        print("[PASS] Both CSV and JSON reports created")

        # Verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            csv_rows = list(reader)
            assert len(csv_rows) == 2, f"Expected 2 CSV rows, got {len(csv_rows)}"

        # Verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            assert len(json_data) == 2, f"Expected 2 JSON items, got {len(json_data)}"

        print("[PASS] Both reports contain correct data")

    print("[PASS] Multiple formats test completed!\n")


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests():
    """Run all integration test functions"""
    print("\n" + "=" * 60)
    print("LABEL DELETE UNUSED INTEGRATION TESTS")
    print("=" * 60 + "\n")

    test_main_no_unused_labels()
    test_main_unused_labels_without_confirm()
    test_main_unused_labels_with_confirm()
    test_main_with_limit()
    test_main_with_deletion_errors()
    test_main_with_json_output()
    test_main_with_multiple_formats()

    print("=" * 60)
    print("ALL INTEGRATION TESTS PASSED!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
