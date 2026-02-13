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

DEFAULT_OUTPUT_DIR = "./output/"


class ReportWriter:
    """
    Handles report generation for CLI commands with support for multiple formats.

    Usage:
       # 1. add typical arguments to the CLI argument parser in the command's fill_parser() function
       ReportWriter.add_arguments_to_parser(parser, default_prefix='my-command')

        # 2. Construct with headers (optionally/recommended pass parsed args to the constructor)
        report_writer = ReportWriter(headers, args=vars(args), filename_prefix='my-command', args=args)

        # 3. Write reports
        report_writer.write_reports()
    """

    def __init__(
        self,
        headers: pylo.ExcelHeaderSet,
        sheet_name: str = "report",
        filename_prefix: Optional[str] = None,
        force_all_wrap_text: bool = True,
        multivalues_cell_delimiter: str = ',',
        args: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the ReportWriter with headers and create an ArraysToExcel workbook + sheet.

        Args:
            headers: Header definitions for the report (required)
            sheet_name: Name for the created Excel sheet
            filename_prefix: Optional filename prefix used when auto-generating filenames
            force_all_wrap_text: Whether to enable wrap text on all cells for the created sheet
            multivalues_cell_delimiter: Delimiter used for multi-value cells
            args: Optional parsed argparse namespace converted to dict (will be used to initialize formats/output settings)
        """
        self.formats: List[ReportFormat] = []
        self.output_dir: str = "output"
        self.output_filename: Optional[str] = None
        self.filename_prefix: Optional[str] = filename_prefix

        # Store sheet configuration
        self.sheet_name: str = sheet_name
        self.headers = headers
        self.force_all_wrap_text = force_all_wrap_text
        self.multivalues_cell_delimiter = multivalues_cell_delimiter

        # Create workbook and the initial sheet
        self.excel_workbook: pylo.ArraysToExcel = pylo.ArraysToExcel()
        self.sheet: pylo.ArraysToExcel.Sheet = self.excel_workbook.create_sheet(
            self.sheet_name,
            self.headers,
            force_all_wrap_text=self.force_all_wrap_text,
            multivalues_cell_delimiter=self.multivalues_cell_delimiter
        )

        # If args were passed to constructor, initialize state from them.
        if args is not None:
            # allow callers to pass either argparse Namespace or dict
            if not isinstance(args, dict):
                try:
                    args = vars(args)
                except Exception:
                    # fall back to expecting a mapping
                    args = dict(args)
            self.initialize_from_args(args)

    @staticmethod
    def add_arguments_to_parser_static(
        parser: argparse.ArgumentParser,
        default_prefix: Optional[str] = None,
        default_sheet_name: str = "report",
        default_format: Optional[ReportFormat] = None,
        format_help: Optional[str] = None,
        output_dir_help: Optional[str] = None,
        output_filename_help: Optional[str] = None,
        populate_arguments_hook: Optional[Callable[[argparse.ArgumentParser], None]] = None
    ):
        """Add standard report-related arguments to an ArgumentParser.

        This static helper only attaches CLI options to the provided parser and
        does not modify any ReportWriter instance state. Typically used in a Command's fill_parser()
        function.
        """
        if format_help is None:
            default_str = default_format if default_format is not None else 'csv'
            format_help = (f'Report format to generate (can be repeated for multiple formats). '
                           f'Default: {default_str}')
        if output_dir_help is None:
            output_dir_help = 'Directory where to write the report file(s)'
        if output_filename_help is None:
            output_filename_help = ('Write report to the specified file (or basename) instead of using the default '
                                   'timestamped filename. If multiple formats are requested, the provided path\'s '
                                   'extension will be replaced/added per format.')

        # Do not set a per-argument default (append + list default leads to merged defaults).
        # Instead, store the desired default on the parser itself so initialize_from_args
        # can decide to use it only when the flag was not provided.
        parser.add_argument('--report-format', '-rf', action='append', type=str,
                            choices=['csv', 'xlsx', 'json'], default=None, help=format_help)
        # Attach parser-level default for later consumption by initialize_from_args
        parser.set_defaults(report_format_default=default_format)
        parser.add_argument(
            '--output-dir', '-o',
            type=str,
            required=False,
            default=DEFAULT_OUTPUT_DIR,
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

    # Expose a convenience instance method that preserves backward compatibility
    @staticmethod
    def add_arguments_to_parser(
        parser: argparse.ArgumentParser,
        default_prefix: Optional[str] = None,
        default_sheet_name: str = "report",
        default_format: Optional[ReportFormat] = None,
        format_help: Optional[str] = None,
        output_dir_help: Optional[str] = None,
        output_filename_help: Optional[str] = None,
        populate_arguments_hook: Optional[Callable[[argparse.ArgumentParser], None]] = None
    ):
        """Static wrapper that registers CLI args.

        Note: this method is intentionally static and does NOT modify any
        ReportWriter instance state. If you previously relied on calling the
        instance method with `default_prefix`/`default_sheet_name` to update
        the instance, please pass `filename_prefix`/`sheet_name` to
        the constructor instead.
        """
        # Delegate to the existing static helper
        ReportWriter.add_arguments_to_parser_static(
            parser,
            default_prefix=default_prefix,
            default_sheet_name=default_sheet_name,
            default_format=default_format,
            format_help=format_help,
            output_dir_help=output_dir_help,
            output_filename_help=output_filename_help,
            populate_arguments_hook=populate_arguments_hook
        )

    def initialize_from_args(
        self,
        args: Dict[str, Any],
    ):
        """
        Initialize the report writer from parsed command-line arguments.

        Args:
            args: Parsed arguments dictionary
        """
        # Get formats from args. If none specified, prefer parser-provided default (report_format_default)
        report_formats = args.get('report_format')
        if report_formats is None or len(report_formats) == 0:
            # parser may have attached a default value under 'report_format_default'
            parser_default = args.get('report_format_default')
            if parser_default is not None:
                # Normalize to list
                if isinstance(parser_default, list):
                    self.formats = parser_default
                else:
                    self.formats = [parser_default]
            else:
                # Fallback to historic default
                self.formats = ['csv']
        else:
            self.formats = report_formats

        self.output_dir = args.get('output_dir', DEFAULT_OUTPUT_DIR)
        self.output_filename = args.get('output_filename')

        # Note: filename_prefix and sheet_name should be set via constructor parameters

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
        sort_by: Optional[List[str]] = None
    ):
        """
        Write reports in all requested formats. If sheet or excel_workbook are not provided,
        the instance's created workbook and sheet will be used.

        Args:
            sort_by: Optional list of column names to sort by before writing

        Note:
            JSON output is now always generated from the current sheet contents. Callers
            should update the `sheet` (or `excel_workbook`) on the ReportWriter instance
            before calling `write_reports` if they need to customize JSON output.
        """
        # Use instance workbook/sheet
        sheet = self.sheet
        excel_workbook = self.excel_workbook

        # Sort data if requested
        if sort_by is not None and sheet is not None:
            sheet.reorder_lines(sort_by)

        # Write each format
        for report_format in self.formats:
            output_filename = self.get_output_filename(report_format)
            self._ensure_directory_exists(output_filename)

            print(f" * Writing report file '{output_filename}' ... ", end='', flush=True)

            try:
                if report_format == 'csv':
                    if sheet is None:
                        raise pylo.PyloEx("sheet is not available for CSV format")
                    sheet.write_to_csv(output_filename)

                elif report_format == 'xlsx':
                    if excel_workbook is None:
                        raise pylo.PyloEx("excel_workbook is not available for XLSX format")
                    excel_workbook.write_to_excel(output_filename)

                elif report_format == 'json':
                    # JSON is now derived from the current sheet contents. Require sheet.
                    if sheet is None:
                        raise pylo.PyloEx("sheet is not available for JSON format")

                    json_data: List[Dict[str, Any]] = []
                    for line in sheet._lines:
                        row_dict: Dict[str, Any] = {}
                        for idx, header in enumerate(sheet._headers):
                            row_dict[header.name] = line[idx]
                        json_data.append(row_dict)

                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2, ensure_ascii=False)

                else:
                    raise pylo.PyloEx(f"Unknown format for report: '{report_format}'")

                print("DONE")

            except Exception as e:
                print(f"ERROR: {e}")
                raise

