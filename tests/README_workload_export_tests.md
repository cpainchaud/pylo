# Workload Export Test Suite

This directory contains tests for the refactored `workload_export.py` utility.

## Test Files

### `test_workload_export.py`
Comprehensive integration tests using actual `workload_export` functions with mock objects.

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

## Running with pytest (if available)

```bash
pytest tests/test_workload_export.py -v
pytest tests/test_workload_export_standalone.py -v
```

## Mock Objects

Tests use mock objects to simulate:
- `MockWorkload` - Workload instances
- `MockVENAgent` - VEN agent instances
- `MockLabel` - Label instances
- `MockInterface` - Network interfaces
- `MockOrganization` - Organization with label store
- `MockFilterData` - CSV/Excel filter data

These mocks avoid requiring a live PCE connection or full pylo environment setup.
