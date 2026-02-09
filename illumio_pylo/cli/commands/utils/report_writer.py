"""
Standardized report writer utility for CLI commands.

This module provides a unified interface for generating reports in multiple formats (CSV, XLSX, JSON)
with consistent filename handling, directory management, and argument parsing.
"""

import argparse
import json
import os
from typing import List, Literal, Optional, Callable, Dict, Any

import illumio_pylo as pylo
from .misc import make_filename_with_timestamp


ReportFormat = Literal['csv', 'xlsx', 'json']


class ReportWriter:
    """
    Handles report generation for CLI commands with support for multiple formats.

    Usage:
        # 1. Add arguments to parser
        report_writer = ReportWriter()
        report_writer.add_arguments_to_parser(parser, default_prefix='my-command')

        # 2. Initialize from parsed arguments
        report_writer.initialize_from_args(args)

        # 3. Write reports
        report_writer.write_reports(sheet, excel_workbook)
    """

    def __init__(self):
        self.formats: List[ReportFormat] = []
        self.output_dir: str = "output"
        self.output_filename: Optional[str] = None
        self.filename_prefix: Optional[str] = None
        self.sheet_name: str = "report"

    def add_arguments_to_parser(
        self,
        parser: argparse.ArgumentParser,
        default_prefix: Optional[str] = None,
        default_sheet_name: str = "report",
        format_help: Optional[str] = None,
        output_dir_help: Optional[str] = None,
        output_filename_help: Optional[str] = None,
        populate_arguments_hook: Optional[Callable[[argparse.ArgumentParser], None]] = None
    ):
        """
        Add standard report-related arguments to an ArgumentParser.

        Args:
            parser: The argument parser to add arguments to
            default_prefix: Default prefix for auto-generated filenames (e.g., 'command-name')
            default_sheet_name: Default Excel sheet name
            format_help: Custom help text for --report-format option
            output_dir_help: Custom help text for --output-dir option
            output_filename_help: Custom help text for --output-filename option
            populate_arguments_hook: Optional callback to add custom arguments
        """
        self.filename_prefix = default_prefix
        self.sheet_name = default_sheet_name

        if format_help is None:
            format_help = 'Report format to generate (can be repeated for multiple formats). Default: csv'
        if output_dir_help is None:
            output_dir_help = 'Directory where to write the report file(s)'
        if output_filename_help is None:
            output_filename_help = ('Write report to the specified file (or basename) instead of using the default '
                                   'timestamped filename. If multiple formats are requested, the provided path\'s '
                                   'extension will be replaced/added per format.')

        parser.add_argument(
            '--report-format', '-rf',
            action='append',
            type=str,
            choices=['csv', 'xlsx', 'json'],
            default=None,
            help=format_help
        )
        parser.add_argument(
            '--output-dir', '-o',
            type=str,
            required=False,
            default="output",
            help=output_dir_help
        )
        parser.add_argument(
            '--output-filename',
            type=str,
            default=None,
            help=output_filename_help
        )

        # Call hook if provided for command-specific arguments
        if populate_arguments_hook is not None:
            populate_arguments_hook(parser)

    def initialize_from_args(
        self,
        args: Dict[str, Any],
        filename_prefix: Optional[str] = None,
        sheet_name: Optional[str] = None
    ):
        """
        Initialize the report writer from parsed command-line arguments.

        Args:
            args: Parsed arguments dictionary
            filename_prefix: Override the filename prefix (if not provided during add_arguments_to_parser)
            sheet_name: Override the sheet name (if not provided during add_arguments_to_parser)
        """
        # Get formats from args, default to ['csv'] if none specified
        report_formats = args.get('report_format')
        if report_formats is None or len(report_formats) == 0:
            self.formats = ['csv']
        else:
            self.formats = report_formats

        self.output_dir = args.get('output_dir', 'output')
        self.output_filename = args.get('output_filename')

        # Override prefix and sheet name if provided
        if filename_prefix is not None:
            self.filename_prefix = filename_prefix
        if sheet_name is not None:
            self.sheet_name = sheet_name

    def get_output_filename(self, report_format: ReportFormat) -> str:
        """
        Generate the output filename for a specific format.

        Args:
            report_format: The format to generate filename for

        Returns:
            Full path to the output file
        """
        if self.output_filename is None:
            # Generate timestamped filename
            if self.filename_prefix is None:
                raise pylo.PyloEx("filename_prefix must be set before calling get_output_filename")

            output_file_prefix = make_filename_with_timestamp(self.filename_prefix + '_', self.output_dir)
            return output_file_prefix + '.' + report_format
        else:
            # Use provided filename
            if len(self.formats) == 1:
                # Single format: use filename as-is, but prepend directory if it's not an absolute path
                if os.path.isabs(self.output_filename):
                    return self.output_filename
                else:
                    return os.path.join(self.output_dir, self.output_filename)
            else:
                # Multiple formats: replace extension
                base = os.path.splitext(self.output_filename)[0]
                filename = base + '.' + report_format
                if os.path.isabs(filename):
                    return filename
                else:
                    return os.path.join(self.output_dir, filename)

    def _ensure_directory_exists(self, filename: str):
        """Ensure the parent directory of a file exists."""
        output_directory = os.path.dirname(filename)
        if output_directory:
            os.makedirs(output_directory, exist_ok=True)

    def write_reports(
        self,
        sheet: Optional[pylo.ArraysToExcel.Sheet] = None,
        excel_workbook: Optional[pylo.ArraysToExcel] = None,
        json_data: Optional[List[Dict[str, Any]]] = None,
        sort_by: Optional[List[str]] = None
    ):
        """
        Write reports in all requested formats.

        Args:
            sheet: The Excel sheet object (required for CSV and XLSX formats)
            excel_workbook: The Excel workbook object (required for XLSX format)
            json_data: List of dictionaries for JSON format (required for JSON format)
            sort_by: Optional list of column names to sort by before writing
        """
        # Sort data if requested
        if sort_by is not None and sheet is not None:
            sheet.reorder_lines(sort_by)

            # Regenerate JSON data from sorted sheet if JSON format is requested
            if 'json' in self.formats and json_data is not None and len(json_data) > 0:
                json_data = []
                for line in sheet._lines:
                    row_dict = {}
                    for idx, header in enumerate(sheet._headers):
                        row_dict[header.name] = line[idx]
                    json_data.append(row_dict)

        # Write each format
        for report_format in self.formats:
            output_filename = self.get_output_filename(report_format)
            self._ensure_directory_exists(output_filename)

            print(f" * Writing report file '{output_filename}' ... ", end='', flush=True)

            try:
                if report_format == 'csv':
                    if sheet is None:
                        raise pylo.PyloEx("sheet parameter is required for CSV format")
                    sheet.write_to_csv(output_filename)

                elif report_format == 'xlsx':
                    if excel_workbook is None:
                        raise pylo.PyloEx("excel_workbook parameter is required for XLSX format")
                    excel_workbook.write_to_excel(output_filename)

                elif report_format == 'json':
                    if json_data is None:
                        raise pylo.PyloEx("json_data parameter is required for JSON format")
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2, ensure_ascii=False)

                else:
                    raise pylo.PyloEx(f"Unknown format for report: '{report_format}'")

                print("DONE")

            except Exception as e:
                print(f"ERROR: {e}")
                raise


def create_standard_report_structure(
    sheet_name: str,
    headers: pylo.ExcelHeaderSet,
    force_all_wrap_text: bool = True,
    multivalues_cell_delimiter: str = ','
) -> tuple[pylo.ArraysToExcel, pylo.ArraysToExcel.Sheet]:
    """
    Create a standard Excel report structure with a single sheet.

    Args:
        sheet_name: Name of the Excel sheet
        headers: Header definitions for the report
        force_all_wrap_text: Whether to wrap text in all cells
        multivalues_cell_delimiter: Delimiter for multi-value cells

    Returns:
        Tuple of (excel_workbook, sheet)
    """
    report = pylo.ArraysToExcel()
    sheet = report.create_sheet(
        sheet_name,
        headers,
        force_all_wrap_text=force_all_wrap_text,
        multivalues_cell_delimiter=multivalues_cell_delimiter
    )
    return report, sheet

