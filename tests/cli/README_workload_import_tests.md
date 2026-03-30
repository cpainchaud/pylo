# Workload Import Test Suite

This directory contains comprehensive tests for the `workload_import.py` utility, including both unit tests and full integration tests.

## Test Files

### `test_workload_import.py`
Unit tests for utility functions with mock objects.

**Prerequisites:**
```bash
pip install -r requirements.txt
```

**Run:**
```bash
python tests/test_workload_import.py
```

**Tests covered:**
- `prepare_workload_creation_data()` - Workload draft creation from CSV data
  - Basic workload with all fields
  - Workload with labels
  - Missing required fields (hostname validation)
  - Multiple interfaces
  - Label assignment and validation

- `detect_workloads_name_collisions()` - Name/hostname collision detection
  - Name collisions with existing workloads
  - Hostname collisions with existing workloads
  - Collision ignoring with flags
  - Multiple collision scenarios

- `detect_ip_collisions()` - IP address collision detection
  - IP collisions with existing workloads
  - Multiple IP handling (comma-separated)
  - Empty IP handling and ignoring
  - Invalid IP format validation

### `test_workload_import_integration.py`
Full integration tests simulating the complete `__main()` command flow with mock PCE data.

**Run:**
```bash
python tests/test_workload_import_integration.py
```

**Integration tests covered:**
1. **Basic Import** - Import workloads without collisions
   - Creates mock organization with label types
   - Validates workload creation from CSV data
   - Tests label assignment and interface creation

2. **Collision Handling** - Test name/hostname collision scenarios
   - Existing workloads in PCE
   - CSV data with name collisions
   - CSV data with hostname collisions
   - Verifies only non-colliding workloads are created

3. **Empty IP Handling** - Test empty IP address handling
   - CSV data with empty IP addresses
   - Tests `--ignore-empty-ip-entries` flag
   - Verifies workloads with empty IPs are ignored

### `test_fixtures.py`
Shared mock objects used across all test files. See `README_test_fixtures.md` for detailed documentation.

## Running All Tests

Run both unit and integration tests:
```bash
python tests/test_workload_import.py && python tests/test_workload_import_integration.py
```

## Running with pytest (if available)

```bash
pytest tests/test_workload_import*.py -v
```

## Mock Objects

Tests use mock objects from `test_fixtures.py` to simulate:
- `MockWorkload` - Workload instances with labels, interfaces
- `MockLabel` - Label instances with proper inheritance
- `MockInterface` - Network interfaces with IP addresses
- `MockOrganization` - Organization with label store and label types
- `MockCSVData` - CSV/Excel data with rows and headers
- `MockWorkloadStore` - Workload store with href-based access
- `MockUnmanagedWorkloadDraftMultiCreatorManager` - Workload creation manager
- `MockReportWriter` - Report generation for CSV/XLSX/JSON formats

These mocks avoid requiring a live PCE connection or full environment setup.

## Test Coverage Summary

The test suite covers:
- ✅ Workload creation data preparation
- ✅ Name/hostname collision detection
- ✅ IP address collision detection
- ✅ Empty IP handling
- ✅ Label assignment and validation
- ✅ Interface creation
- ✅ Error handling and validation
- ✅ Full command execution flow
- ✅ Report generation

## Patterns Demonstrated

1. **Pure Function Extraction**: Core logic extracted into testable functions
2. **Structured Returns**: Functions return meaningful objects for assertions
3. **Mock API Objects**: Realistic mocks that inherit from actual pylo classes
4. **Comprehensive Edge Cases**: Empty inputs, collisions, invalid data
5. **Integration Testing**: Full command flow with mocked dependencies
6. **Error Scenario Testing**: Validation failures, collision handling

## Related Files

- `illumio_pylo/cli/commands/workload_import.py` - The command implementation
- `test_fixtures.py` - Shared test fixtures and mock classes
- `conftest.py` - Pytest configuration
- `README_test_fixtures.md` - Test fixtures documentation