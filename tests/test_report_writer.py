"""
Test script for the ReportWriter utility.

This script tests the report writer functionality including:
- Argument parsing
- Filename generation
- Multiple format support
- Custom filename handling
- Report writing in CSV, XLSX, and JSON formats
"""
import argparse
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
from illumio_pylo.cli.commands.utils.report_writer import ReportWriter


def test_argument_parsing():
    """Test that ReportWriter correctly adds and parses arguments"""
    print("=" * 60)
    print("Testing Argument Parsing")
    print("=" * 60)

    # Test 1: Default arguments
    print("\nTest 1: Default arguments (no format specified)")
    parser = argparse.ArgumentParser()
    # Register arguments using the static helper and explicitly pass the
    # desired defaults into initialize_from_args since the static helper no
    # longer mutates instance state.
    ReportWriter.add_arguments_to_parser(
        parser,
        default_prefix='test-command',
        default_sheet_name='test_sheet'
    )

    args = parser.parse_args([])
    args_dict = vars(args)
    # Construct ReportWriter with the expected defaults and parsed args
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), filename_prefix='test-command', sheet_name='test_sheet', args=args_dict)

    assert report_writer.formats == ['csv'], f"Expected ['csv'], got {report_writer.formats}"
    assert report_writer.output_dir == 'output', f"Expected 'output', got {report_writer.output_dir}"
    assert report_writer.output_filename is None, f"Expected None, got {report_writer.output_filename}"
    assert report_writer.filename_prefix == 'test-command', f"Expected 'test-command', got {report_writer.filename_prefix}"
    assert report_writer.sheet_name == 'test_sheet', f"Expected 'test_sheet', got {report_writer.sheet_name}"
    print("  [OK] Default arguments parsed correctly")

    # Test 2: Single format specified
    print("\nTest 2: Single format (xlsx)")
    parser = argparse.ArgumentParser()
    ReportWriter.add_arguments_to_parser(parser, default_prefix='test')
    args = parser.parse_args(['--report-format', 'xlsx'])
    args_dict = vars(args)
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), args=args_dict)

    assert report_writer.formats == ['xlsx'], f"Expected ['xlsx'], got {report_writer.formats}"
    print("  [OK] Single format parsed correctly")

    # Test 3: Multiple formats specified
    print("\nTest 3: Multiple formats (csv, xlsx, json)")
    parser = argparse.ArgumentParser()
    ReportWriter.add_arguments_to_parser(parser, default_prefix='test')
    args = parser.parse_args(['-rf', 'csv', '-rf', 'xlsx', '-rf', 'json'])
    args_dict = vars(args)
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), args=args_dict)

    assert report_writer.formats == ['csv', 'xlsx', 'json'], f"Expected ['csv', 'xlsx', 'json'], got {report_writer.formats}"
    print("  [OK] Multiple formats parsed correctly")

    # Test 4: Custom output directory
    print("\nTest 4: Custom output directory")
    parser = argparse.ArgumentParser()
    ReportWriter.add_arguments_to_parser(parser, default_prefix='test')
    args = parser.parse_args(['--output-dir', '/tmp/reports'])
    args_dict = vars(args)
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), args=args_dict)

    assert report_writer.output_dir == '/tmp/reports', f"Expected '/tmp/reports', got {report_writer.output_dir}"
    print("  [OK] Custom output directory parsed correctly")

    # Test 5: Custom filename
    print("\nTest 5: Custom filename")
    parser = argparse.ArgumentParser()
    ReportWriter.add_arguments_to_parser(parser, default_prefix='test')
    args = parser.parse_args(['--output-filename', 'myreport.csv'])
    args_dict = vars(args)
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), args=args_dict)

    assert report_writer.output_filename == 'myreport.csv', f"Expected 'myreport.csv', got {report_writer.output_filename}"
    print("  [OK] Custom filename parsed correctly")

    print("\n[OK] All argument parsing tests passed!")


def test_default_format_behavior():
    """Test that providing `default_format` to the argument helper sets the parser default

    Happy path: when no --report-format is passed, parser should have the default value.
    Edge case: when an explicit --report-format is passed it should override the default.
    """
    print("\n" + "=" * 60)
    print("Testing default_format behavior")
    print("=" * 60)

    # Happy path: default_format='json' should become the parser default
    parser = argparse.ArgumentParser()
    ReportWriter.add_arguments_to_parser(parser, default_format='json')

    args = parser.parse_args([])
    args_dict = vars(args)
    # The parser stores the provided default at the parser level (report_format_default)
    # and the per-argument value remains None when the flag is not passed.
    assert args_dict.get('report_format') is None, f"Expected no explicit report_format in args, got {args_dict.get('report_format')}"
    assert args_dict.get('report_format_default') == 'json', f"Expected parser-level default 'json', got {args_dict.get('report_format_default')}"

    # initialize_from_args should pick up the parser-level default when the flag wasn't provided
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), args=args_dict)
    assert report_writer.formats == ['json'], f"Expected initialized formats ['json'], got {report_writer.formats}"
    print("  [OK] default_format provided is used when no flag is passed")

    # Edge case: explicit flag should override the default_format
    parser = argparse.ArgumentParser()
    ReportWriter.add_arguments_to_parser(parser, default_format='csv')

    args = parser.parse_args(['--report-format', 'xlsx'])
    args_dict = vars(args)
    # When an explicit flag is provided, it should appear in report_format and override the parser default
    assert args_dict.get('report_format') == ['xlsx'], f"Expected explicit ['xlsx'] in args, got {args_dict.get('report_format')}"
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), args=args_dict)
    assert report_writer.formats == ['xlsx'], f"Expected ['xlsx'] when explicit flag provided, got {report_writer.formats}"
    print("  [OK] Explicit --report-format overrides default_format")

    print("\n[OK] default_format behavior tests passed!")


def test_filename_generation():
    """Test filename generation logic"""
    print("\n" + "=" * 60)
    print("Testing Filename Generation")
    print("=" * 60)

    # Test 1: Auto-generated filenames
    print("\nTest 1: Auto-generated filenames")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['csv', 'xlsx', 'json']
    report_writer.output_dir = 'output'
    report_writer.output_filename = None
    report_writer.filename_prefix = 'test-command'

    csv_filename = report_writer.get_output_filename('csv')
    xlsx_filename = report_writer.get_output_filename('xlsx')
    json_filename = report_writer.get_output_filename('json')

    assert csv_filename.endswith('.csv'), f"CSV filename should end with .csv: {csv_filename}"
    assert xlsx_filename.endswith('.xlsx'), f"XLSX filename should end with .xlsx: {xlsx_filename}"
    assert json_filename.endswith('.json'), f"JSON filename should end with .json: {json_filename}"
    assert 'test-command' in csv_filename, f"Filename should contain prefix: {csv_filename}"
    print(f"  [OK] CSV:  {csv_filename}")
    print(f"  [OK] XLSX: {xlsx_filename}")
    print(f"  [OK] JSON: {json_filename}")

    # Test 2: Custom filename with single format
    print("\nTest 2: Custom filename with single format")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['csv']
    report_writer.output_dir = 'output'
    report_writer.output_filename = 'myreport.csv'

    filename = report_writer.get_output_filename('csv')
    expected = os.path.join('output', 'myreport.csv')
    assert filename == expected, f"Expected '{expected}', got {filename}"
    print(f"  [OK] Single format: {filename}")

    # Test 3: Custom filename with multiple formats
    print("\nTest 3: Custom filename with multiple formats")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['csv', 'xlsx', 'json']
    report_writer.output_dir = 'output'
    report_writer.output_filename = 'myreport.csv'

    csv_filename = report_writer.get_output_filename('csv')
    xlsx_filename = report_writer.get_output_filename('xlsx')
    json_filename = report_writer.get_output_filename('json')

    expected_csv = os.path.join('output', 'myreport.csv')
    expected_xlsx = os.path.join('output', 'myreport.xlsx')
    expected_json = os.path.join('output', 'myreport.json')

    assert csv_filename == expected_csv, f"Expected '{expected_csv}', got {csv_filename}"
    assert xlsx_filename == expected_xlsx, f"Expected '{expected_xlsx}', got {xlsx_filename}"
    assert json_filename == expected_json, f"Expected '{expected_json}', got {json_filename}"
    print(f"  [OK] CSV:  {csv_filename}")
    print(f"  [OK] XLSX: {xlsx_filename}")
    print(f"  [OK] JSON: {json_filename}")

    # Test 4: Custom filename without extension
    print("\nTest 4: Custom filename without extension")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['csv', 'xlsx']
    report_writer.output_dir = 'output'
    report_writer.output_filename = 'myreport'

    csv_filename = report_writer.get_output_filename('csv')
    xlsx_filename = report_writer.get_output_filename('xlsx')

    expected_csv = os.path.join('output', 'myreport.csv')
    expected_xlsx = os.path.join('output', 'myreport.xlsx')

    assert csv_filename == expected_csv, f"Expected '{expected_csv}', got {csv_filename}"
    assert xlsx_filename == expected_xlsx, f"Expected '{expected_xlsx}', got {xlsx_filename}"
    print(f"  [OK] CSV:  {csv_filename}")
    print(f"  [OK] XLSX: {xlsx_filename}")

    print("\n[OK] All filename generation tests passed!")


def test_report_structure_creation():
    """Test creating standard report structures"""
    print("\n" + "=" * 60)
    print("Testing Report Structure Creation")
    print("=" * 60)

    # Test 1: Simple headers
    print("\nTest 1: Simple headers")
    headers = pylo.ExcelHeaderSet(['name', 'value', 'type'])
    report_writer = ReportWriter(headers=headers, sheet_name='test_sheet')

    report = report_writer.excel_workbook
    sheet = report_writer.sheet

    assert sheet is not None, "Sheet should not be None"
    assert sheet._headers is not None, "Sheet headers should not be None"
    assert len(sheet._headers) == 3, f"Expected 3 headers, got {len(sheet._headers)}"
    print("  [OK] Simple headers created successfully")

    # Test 2: Complex headers with ExcelHeader objects
    print("\nTest 2: Complex headers with ExcelHeader objects")
    headers = pylo.ExcelHeaderSet([
        pylo.ExcelHeader(name='name', max_width=40, wrap_text=False),
        pylo.ExcelHeader(name='description', max_width=60),
        pylo.ExcelHeader(name='url', max_width=15, url_text='Click', is_url=True),
        'simple_column'
    ])
    report_writer = ReportWriter(headers=headers, sheet_name='complex_sheet')

    report = report_writer.excel_workbook
    sheet = report_writer.sheet

    assert sheet is not None, "Sheet should not be None"
    assert len(sheet._headers) == 4, f"Expected 4 headers, got {len(sheet._headers)}"
    print("  [OK] Complex headers created successfully")

    # Test 3: Add data to sheet
    print("\nTest 3: Add data to sheet")
    headers = pylo.ExcelHeaderSet(['col1', 'col2', 'col3'])
    report_writer = ReportWriter(headers=headers, sheet_name='data_sheet')

    report = report_writer.excel_workbook
    sheet = report_writer.sheet

    sheet.add_line_from_object({'col1': 'value1', 'col2': 'value2', 'col3': 'value3'})
    sheet.add_line_from_object({'col1': 'value4', 'col2': 'value5', 'col3': 'value6'})

    assert sheet.lines_count() == 2, f"Expected 2 lines, got {sheet.lines_count()}"
    print("  [OK] Data added to sheet successfully")

    print("\n[OK] All report structure creation tests passed!")


def test_report_writing():
    """Test writing reports in different formats"""
    print("\n" + "=" * 60)
    print("Testing Report Writing")
    print("=" * 60)

    # Create a temporary directory for test outputs
    temp_dir = tempfile.mkdtemp(prefix='pylo_test_')
    print(f"\nUsing temporary directory: {temp_dir}")

    try:
        # Create test data
        headers = pylo.ExcelHeaderSet([
            pylo.ExcelHeader(name='name', max_width=30),
            pylo.ExcelHeader(name='value', max_width=20),
            pylo.ExcelHeader(name='count', max_width=10)
        ])
        data_writer = ReportWriter(headers=headers, sheet_name='test_data')

        report = data_writer.excel_workbook
        sheet = data_writer.sheet

        test_data = [
            {'name': 'Item 1', 'value': 'Value A', 'count': '10'},
            {'name': 'Item 2', 'value': 'Value B', 'count': '20'},
            {'name': 'Item 3', 'value': 'Value C', 'count': '30'}
        ]

        for row in test_data:
            sheet.add_line_from_object(row)

        # Test 1: Write CSV format (use writer that contains the populated sheet)
        print("\nTest 1: Write CSV format")
        data_writer.formats = ['csv']
        data_writer.output_dir = temp_dir
        data_writer.output_filename = 'test_report.csv'

        data_writer.write_reports()

        csv_path = os.path.join(temp_dir, 'test_report.csv')
        assert os.path.exists(csv_path), f"CSV file should exist: {csv_path}"

        # Verify CSV content
        with open(csv_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 4, f"Expected 4 lines (header + 3 data), got {len(lines)}"
            assert 'name' in lines[0], "Header should contain 'name'"
            assert 'Item 1' in lines[1], "First data row should contain 'Item 1'"
        print(f"  [OK] CSV file created and validated: {csv_path}")

        # Test 2: Write XLSX format
        print("\nTest 2: Write XLSX format")
        data_writer.formats = ['xlsx']
        data_writer.output_dir = temp_dir
        data_writer.output_filename = 'test_report.xlsx'

        data_writer.write_reports()

        xlsx_path = os.path.join(temp_dir, 'test_report.xlsx')
        assert os.path.exists(xlsx_path), f"XLSX file should exist: {xlsx_path}"
        assert os.path.getsize(xlsx_path) > 0, "XLSX file should not be empty"
        print(f"  [OK] XLSX file created: {xlsx_path}")

        # Test 3: Write JSON format
        print("\nTest 3: Write JSON format")
        data_writer.formats = ['json']
        data_writer.output_dir = temp_dir
        data_writer.output_filename = 'test_report.json'

        data_writer.write_reports()

        json_path = os.path.join(temp_dir, 'test_report.json')
        assert os.path.exists(json_path), f"JSON file should exist: {json_path}"

        # Verify JSON content
        with open(json_path, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert isinstance(loaded_data, list), "JSON should be a list"
            assert len(loaded_data) == 3, f"Expected 3 records, got {len(loaded_data)}"
            assert loaded_data[0]['name'] == 'Item 1', "First record name should be 'Item 1'"
            assert loaded_data[0]['value'] == 'Value A', "First record value should be 'Value A'"
            assert loaded_data[0]['count'] == '10', "First record count should be '10'"
        print(f"  [OK] JSON file created and validated: {json_path}")

        # Test 4: Write multiple formats
        print("\nTest 4: Write multiple formats (csv, xlsx, json)")
        data_writer.formats = ['csv', 'xlsx', 'json']
        data_writer.output_dir = temp_dir
        data_writer.output_filename = 'multi_format'

        data_writer.write_reports()

        multi_csv = os.path.join(temp_dir, 'multi_format.csv')
        multi_xlsx = os.path.join(temp_dir, 'multi_format.xlsx')
        multi_json = os.path.join(temp_dir, 'multi_format.json')

        assert os.path.exists(multi_csv), f"Multi-format CSV should exist: {multi_csv}"
        assert os.path.exists(multi_xlsx), f"Multi-format XLSX should exist: {multi_xlsx}"
        assert os.path.exists(multi_json), f"Multi-format JSON should exist: {multi_json}"
        print(f"  [OK] All formats created:")
        print(f"    - {multi_csv}")
        print(f"    - {multi_xlsx}")
        print(f"    - {multi_json}")

        # Test 5: Write empty report
        print("\nTest 5: Write empty report")
        empty_headers = pylo.ExcelHeaderSet(['col1', 'col2'])
        empty_writer = ReportWriter(headers=empty_headers, sheet_name='empty')
        empty_report = empty_writer.excel_workbook
        empty_sheet = empty_writer.sheet

        report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
        report_writer.formats = ['csv', 'json']
        report_writer.output_dir = temp_dir
        report_writer.output_filename = 'empty_report'

        # Ensure sheet is empty (no lines added)
        report_writer.write_reports()

        empty_csv = os.path.join(temp_dir, 'empty_report.csv')
        empty_json = os.path.join(temp_dir, 'empty_report.json')

        assert os.path.exists(empty_csv), f"Empty CSV should exist: {empty_csv}"
        assert os.path.exists(empty_json), f"Empty JSON should exist: {empty_json}"

        # Verify empty CSV has headers
        with open(empty_csv, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 1, f"Empty CSV should have 1 line (header only), got {len(lines)}"

        # Verify empty JSON is empty array
        with open(empty_json, 'r') as f:
            data = json.load(f)
            assert data == [], f"Empty JSON should be [], got {data}"

        print(f"  [OK] Empty reports created successfully")

        # Test 6: Test sorting
        print("\nTest 6: Test sorting")
        sort_headers = pylo.ExcelHeaderSet(['type', 'name', 'value'])
        sort_writer = ReportWriter(headers=sort_headers, sheet_name='sorted')
        sort_report = sort_writer.excel_workbook
        sort_sheet = sort_writer.sheet

        sort_data = [
            {'type': 'B', 'name': 'Item 2', 'value': '20'},
            {'type': 'A', 'name': 'Item 3', 'value': '30'},
            {'type': 'A', 'name': 'Item 1', 'value': '10'},
        ]

        for row in sort_data:
            sort_sheet.add_line_from_object(row)

        # Use sort_writer (which contains the populated sheet) to write sorted JSON
        sort_writer.formats = ['json']
        sort_writer.output_dir = temp_dir
        sort_writer.output_filename = 'sorted_report.json'

        sort_writer.write_reports(sort_by=['type', 'name'])

        sorted_json = os.path.join(temp_dir, 'sorted_report.json')
        with open(sorted_json, 'r') as f:
            sorted_result = json.load(f)
            assert sorted_result[0]['type'] == 'A', "First record should be type A"
            assert sorted_result[0]['name'] == 'Item 1', "First A record should be Item 1"
            assert sorted_result[1]['name'] == 'Item 3', "Second A record should be Item 3"
            assert sorted_result[2]['type'] == 'B', "Third record should be type B"

        print(f"  [OK] Sorting works correctly")

        print("\n[OK] All report writing tests passed!")

    finally:
        # Clean up temporary directory (but only after all assertions pass)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\nCleaned up temporary directory: {temp_dir}")


def test_error_handling():
    """Test error handling in ReportWriter"""
    print("\n" + "=" * 60)
    print("Testing Error Handling")
    print("=" * 60)

    # Test 1: Missing filename_prefix
    print("\nTest 1: Missing filename_prefix when needed")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['csv']
    report_writer.output_dir = 'output'
    report_writer.output_filename = None
    report_writer.filename_prefix = None  # Not set!

    try:
        report_writer.get_output_filename('csv')
        assert False, "Should have raised PyloEx"
    except pylo.PyloEx as e:
        assert 'filename_prefix' in str(e), f"Error message should mention filename_prefix: {e}"
        print("  [OK] Correctly raises error when filename_prefix is missing")

    # Test 2: Missing sheet parameter for CSV
    print("\nTest 2: Missing sheet parameter for CSV format")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['csv']
    report_writer.output_dir = 'output'
    report_writer.output_filename = 'test.csv'

    try:
        # Simulate missing sheet by removing it from the ReportWriter instance
        # noinspection PyTypeChecker
        report_writer.sheet = None
        report_writer.write_reports()
        assert False, "Should have raised PyloEx"
    except pylo.PyloEx as e:
        assert 'sheet' in str(e).lower(), f"Error message should mention sheet: {e}"
        print("  [OK] Correctly raises error when sheet is missing for CSV")

    # Test 3: Missing excel_workbook parameter for XLSX
    print("\nTest 3: Missing excel_workbook parameter for XLSX format")
    headers = pylo.ExcelHeaderSet(['col1'])
    temp_writer = ReportWriter(headers=headers, sheet_name='test')
    _, sheet = temp_writer.excel_workbook, temp_writer.sheet

    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['xlsx']
    report_writer.output_dir = 'output'
    report_writer.output_filename = 'test.xlsx'

    try:
        # Simulate missing excel_workbook by removing it from the instance
        # noinspection PyTypeChecker
        report_writer.excel_workbook = None
        report_writer.write_reports()
        assert False, "Should have raised PyloEx"
    except pylo.PyloEx as e:
        assert 'excel_workbook' in str(e).lower(), f"Error message should mention excel_workbook: {e}"
        print("  [OK] Correctly raises error when excel_workbook is missing for XLSX")

    # Test 4: Missing json_data parameter for JSON
    print("\nTest 4: Missing json_data parameter for JSON format")
    report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]))
    report_writer.formats = ['json']
    report_writer.output_dir = 'output'
    report_writer.output_filename = 'test.json'

    try:
        # Simulate missing sheet for JSON format (json_data parameter removed)
        # noinspection PyTypeChecker
        report_writer.sheet = None
        report_writer.write_reports()
        assert False, "Should have raised PyloEx"
    except pylo.PyloEx as e:
        assert 'sheet' in str(e).lower(), f"Error message should mention sheet: {e}"
        print("  [OK] Correctly raises error when sheet is missing for JSON")

    print("\n[OK] All error handling tests passed!")


def test_integration():
    """Integration test simulating real CLI command usage"""
    print("\n" + "=" * 60)
    print("Testing Integration (Real CLI Simulation)")
    print("=" * 60)

    temp_dir = tempfile.mkdtemp(prefix='pylo_integration_test_')
    print(f"\nUsing temporary directory: {temp_dir}")

    try:
        # Simulate a CLI command processing
        print("\nSimulating: pylo my-command -rf csv -rf json -o temp_dir --output-filename report")

        # Step 1: Parse arguments
        parser = argparse.ArgumentParser()
        ReportWriter.add_arguments_to_parser(
            parser,
            default_prefix='my-command',
            default_sheet_name='my_data'
        )

        args = parser.parse_args([
             '-rf', 'csv',
             '-rf', 'json',
             '-o', temp_dir,
             '--output-filename', 'report'
         ])

        # Step 2: Initialize report writer. Pass filename_prefix/sheet_name so
        # the instance picks up the expected defaults (the static arg parser
        # helper does not mutate instance state).
        report_writer = ReportWriter(headers=pylo.ExcelHeaderSet([]), filename_prefix='my-command', sheet_name='my_data', args=vars(args))

        # Step 3: Create report structure
        headers = pylo.ExcelHeaderSet([
            pylo.ExcelHeader(name='id', max_width=10),
            pylo.ExcelHeader(name='name', max_width=30),
            pylo.ExcelHeader(name='status', max_width=15),
            pylo.ExcelHeader(name='description', max_width=50)
        ])

        # Create report writer bound to the real headers and recreate sheet
        real_writer = ReportWriter(headers=headers, sheet_name='my_data', filename_prefix='my-command', force_all_wrap_text=True, multivalues_cell_delimiter=',', args=vars(args))
        report = real_writer.excel_workbook
        sheet = real_writer.sheet

        # Step 4: Add data (simulating command processing)
        sample_data = [
            {'id': '1', 'name': 'Server-01', 'status': 'active', 'description': 'Production web server'},
            {'id': '2', 'name': 'Server-02', 'status': 'inactive', 'description': 'Backup database server'},
            {'id': '3', 'name': 'Server-03', 'status': 'active', 'description': 'Application server'},
            {'id': '4', 'name': 'Server-04', 'status': 'maintenance', 'description': 'Load balancer'},
        ]

        for item in sample_data:
            sheet.add_line_from_object(item)

        # Step 6: Write reports
        real_writer.write_reports(sort_by=['status', 'name'])

        # Step 7: Verify outputs
        csv_file = os.path.join(temp_dir, 'report.csv')
        json_file = os.path.join(temp_dir, 'report.json')

        assert os.path.exists(csv_file), f"CSV file should exist: {csv_file}"
        assert os.path.exists(json_file), f"JSON file should exist: {json_file}"

        # Verify CSV content
        with open(csv_file, 'r') as f:
            csv_lines = f.readlines()
            assert len(csv_lines) == 5, f"Expected 5 lines (1 header + 4 data), got {len(csv_lines)}"

        # Verify JSON content
        with open(json_file, 'r') as f:
            json_content = json.load(f)
            assert len(json_content) == 4, f"Expected 4 records, got {len(json_content)}"
            # Check sorting worked (by status, then name)
            assert json_content[0]['status'] == 'active', "First record should be 'active'"
            assert json_content[0]['name'] == 'Server-01', "First active should be Server-01"

        print("  [OK] Integration test completed successfully")
        print(f"    Created files:")
        print(f"      - {csv_file}")
        print(f"      - {json_file}")

    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\nCleaned up temporary directory: {temp_dir}")

    print("\n[OK] Integration test passed!")


def run_all_tests():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("PYLO REPORT WRITER TEST SUITE")
    print("=" * 60)

    tests = [
        ("Argument Parsing", test_argument_parsing),
        ("Default Format Behavior", test_default_format_behavior),
        ("Filename Generation", test_filename_generation),
        ("Report Structure Creation", test_report_structure_creation),
        ("Report Writing", test_report_writing),
        ("Error Handling", test_error_handling),
        ("Integration Test", test_integration)
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"\n[FAILED] {test_name}: {e}")
            failed += 1
        except Exception as e:
            print(f"\n[ERROR] {test_name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed == 0:
        print("\n*** ALL TESTS PASSED! ***")
        return 0
    else:
        print(f"\n*** {failed} test(s) failed ***")
        return 1


if __name__ == '__main__':
    sys.exit(run_all_tests())

