# IPList Analyzer Test Suite

This directory contains comprehensive tests for the refactored `iplist_analyzer.py` command, including both unit tests and full integration tests.

## Test Files

### `test_iplist_analyzer.py`
Unit tests for utility functions with mock objects.

**Prerequisites:**
```bash
pip install -r requirements.txt
```

**Run:**
```bash
python tests/cli/test_iplist_analyzer.py
```

**Tests covered:**
- `build_workloads_ip4_cache()` - Workload IP4 cache building
  - Cache with multiple workloads
  - Empty workload list handling
- `build_iplists_ip4_cache()` - IPList IP4 cache building
  - Cache with multiple iplists
  - Empty iplist handling
- `analyze_iplist_coverage()` - IPList coverage analysis
  - Matching workloads detection
  - No matching workloads
  - Empty workload cache
  - App group tracking
- `add_iplist_analysis_to_report()` - Report row generation
  - Complete analysis result
  - Empty analysis result (no matches)
  - IP count calculations
  - Workload and appgroup formatting

### `test_iplist_analyzer_integration.py`
Full integration tests simulating the complete `__main()` command flow with mock organization.

**Run:**
```bash
python tests/cli/test_iplist_analyzer_integration.py
```

**Integration tests covered:**
1. **Basic Analysis** - Full IPList analysis workflow
   - Creates 3 workloads and 3 iplists
   - Validates coverage matching (2 workloads, 1 workload, 0 workloads)
   - Validates CSV report generation

2. **JSON Output Format** - Test JSON report generation
   - Validates JSON file creation
   - Validates JSON structure and content

3. **Multiple Output Formats** - Test simultaneous multi-format output
   - Generates CSV, JSON, and XLSX files
   - Validates all formats created with correct data

4. **No Workloads** - Test with empty workload list
   - IPList analyzed with zero coverage
   - All IPs reported as uncovered

5. **No IPLists** - Test with empty IPList store
   - Empty report generation
   - Warning message for empty results

### `test_fixtures.py`
Shared mock objects used across all test files. See `README_test_fixtures.md` for detailed documentation.

## Running All Tests

Run both unit and integration tests:
```bash
python tests/cli/test_iplist_analyzer.py && python tests/cli/test_iplist_analyzer_integration.py
```

## Running with pytest (if available)

```bash
pytest tests/cli/test_iplist_analyzer*.py -v
```

## Mock Objects

Tests use custom mock objects specific to IP analysis:
- `MockIP4Map` - IP4 map with range tracking and subtraction simulation
- `MockIPList` - IPList with configurable entries and IP maps
- `MockWorkloadForIPTest` - Extended workload with IP4 map support
- `MockOrganizationForIPTest` - Organization with workload/iplist stores
- `MockSheet` - Excel sheet for report generation testing

These mocks avoid requiring a live PCE connection or complex IP mapping logic.

## Refactoring Benefits

The refactored `iplist_analyzer.py` code is now highly testable:

1. **Separated cache building** - `build_workloads_ip4_cache()` and `build_iplists_ip4_cache()` can be tested independently
2. **Isolated analysis logic** - `analyze_iplist_coverage()` contains pure business logic
3. **Decoupled report writing** - `add_iplist_analysis_to_report()` accepts structured results
4. **ReportWriter integration** - Standardized report generation with multiple formats
5. **Testable `__main()`** - Can mock Organization and test end-to-end

## Test Coverage Summary

- **Unit tests**: 4 test functions covering all extracted business logic functions
- **Integration tests**: 5 test functions covering all user scenarios and edge cases
- **Total assertions**: 60+ assertions validating behavior
- **All tests passing**: ✅ 100% success rate

## Command-Specific Features Tested

- IP4 map caching for workloads and iplists
- Coverage analysis (IP overlap detection)
- Uncovered IP calculation (subtraction logic)
- App group tracking
- Report generation (CSV, JSON, XLSX, multiple formats)
- Empty workload/iplist handling
- Warning messages for empty reports

## Key Differences from Other Commands

Unlike simpler commands, `iplist_analyzer` deals with:
- **IP4 mapping logic**: Requires IP range comparison and subtraction
- **Complex object relationships**: Workloads, IPLists, and IP maps interact
- **Stateful operations**: IP subtraction modifies IP4Map state during analysis
- **No user filters**: Analyzes all iplists against all workloads automatically

These complexities required more sophisticated mocks (MockIP4Map with subtraction simulation) to properly test the logic.

## Usage Documentation

End-user documentation for this command is located in `/docs/cli/iplist-analyzer.md`.
