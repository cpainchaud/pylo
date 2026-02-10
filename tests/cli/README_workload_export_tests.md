# Workload Export Test Suite

This directory contains comprehensive tests for the refactored `workload_export.py` utility, including both unit tests and full integration tests.

## Test Files

### `test_workload_export.py`
Unit tests for utility functions with mock objects.

**Prerequisites:**
```bash
pip install -r requirements.txt
```

**Run:**
```bash
python tests/test_workload_export.py
```

**Tests covered:**
- `format_date_or_none()` - Date formatting with None handling
- `build_workload_row()` - Workload to dictionary conversion
  - Basic workloads
  - Workloads with VEN agents
  - Unmanaged workloads
- `match_filter_row_against_workload()` - Filter matching
  - Hostname matching (FQDN vs short name)
  - App label matching (case insensitive)
  - IP address matching (single and multiple interfaces)
  - Multiple filter fields (AND logic)
  - Unsupported field error handling
- `build_report_headers()` - Header construction
  - Standard label types
  - Custom label types
  - Extra column support
- `find_matching_filters_for_workload()` - Finding all matching filters
  - Single field filtering
  - Multiple field filtering
  - No matches handling
  - Error propagation
- `ExtraColumnRegistry` - Extensibility pattern
  - Registration
  - Retrieval
  - Clearing
  - Copy protection
- `test_report_writing()` - Report generation
  - CSV format output
  - XLSX format output
  - JSON format output
  - Multiple formats simultaneously
  - Empty report handling

### `test_workload_export_integration.py`
Full integration tests simulating the complete `__main()` command flow with mock PCE data.

**Run:**
```bash
python tests/test_workload_export_integration.py
```

**Integration tests covered:**
1. **Basic Export** - Export all workloads without filters
   - Creates mock organization with 5 workloads
   - Validates CSV output with all workloads

2. **Filter Query** - Test filter query functionality
   - Query: `env == 'Production' and role == 'Web'`
   - Validates filtered results (2 workloads)

3. **Filter File** - Test CSV filter file functionality
   - Filters by hostname and app fields
   - Validates matched workloads

4. **Filter File with Keep in Report** - Test keep_filters_in_report option
   - Includes filter columns in output (_hostname, _app, _owner)
   - Adds unmatched filters as rows
   - Validates matched + unmatched filter rows

5. **Multiple Formats** - Test simultaneous multi-format output
   - Generates CSV, XLSX, and JSON files
   - Validates all formats created

6. **Empty Result** - Test empty result handling
   - Filter matching no workloads
   - Validates empty CSV with headers

### `test_fixtures.py`
Shared mock objects used across all test files. See `README_test_fixtures.md` for detailed documentation.

## Running All Tests

Run both unit and integration tests:
```bash
python tests/test_workload_export.py && python tests/test_workload_export_integration.py
```

## Running with pytest (if available)

```bash
pytest tests/test_workload_export*.py -v
```

## Mock Objects

Tests use mock objects from `test_fixtures.py` to simulate:
- `MockWorkload` - Workload instances with labels, interfaces, VEN agents
- `MockVENAgent` - VEN agent instances with heartbeat and sync state
- `MockLabel` - Label instances with proper inheritance
- `MockInterface` - Network interfaces with IP addresses
- `MockOrganization` - Organization with label store and label types
- `MockFilterData` - CSV/Excel filter data
- `MockWorkloadStore` - Workload store with href-based access

These mocks avoid requiring a live PCE connection or full environment setup.

## Bug Fixes

The integration tests discovered and helped fix a bug in `workload_export.py` where the ReportWriter was created before filter file headers were added, causing a `KeyError` when using `--keep-filters-in-report`. The fix moved ReportWriter creation to occur after all headers are finalized.
