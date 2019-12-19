import pylo.vendors.xlsxwriter as xlsxwriter
import pylo.vendors.openpyxl as openpyxl
import csv
import pylo
import os


class ArrayToExport:
    def __init__(self, headers):
        self._headers = headers
        self._columns = len(headers)
        self._lines = []

        self._headers_name_to_index = {}
        self._headers_index_to_name = []
        index = 0
        for header_name in headers:
            self._headers_name_to_index[header_name] = index
            self._headers_index_to_name.append(header_name)
            index += 1


    def add_line_from_list(self, line: list):
        if len(line) != self._columns:
            raise pylo.PyloEx("line length ({}) does not match the number of columns ({})".format(len(line), self._columns))
        self._lines.append(line)

    def write_to_csv(self, filename, delimiter=',', multivalues_cell_delimiter=' '):
        with open(filename, 'w', newline='') as csv_file:
            filewriter = csv.writer(csv_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_ALL)
            filewriter.writerow(self._headers)
            for line in self._lines:
                new_line = []
                for item in line:
                    if type(item) is list:
                        new_line.append(pylo.string_list_to_text(item, multivalues_cell_delimiter))
                    else:
                        new_line.append(item)
                filewriter.writerow(new_line)

    def write_to_excel(self, filename, worksheet_name='worksheet1', multivalues_cell_delimiter=' '):
        xls_workbook = xlsxwriter.Workbook(filename)
        cell_format = xls_workbook.add_format()
        cell_format.set_text_wrap()
        cell_format.set_valign('vcenter')
        xls_worksheet = xls_workbook.add_worksheet(worksheet_name)
        xls_headers = []
        xls_data = []
        header_index = 0
        for header in self._headers:
            xls_headers.append({'header': header, 'format': cell_format})
            header_index += 1

        for line in self._lines:
            new_line = []
            for item in line:
                if type(item) is list:
                    new_line.append(pylo.string_list_to_text(item, multivalues_cell_delimiter))
                else:
                    new_line.append(item)
            xls_data.append(new_line)

        xls_table = xls_worksheet.add_table(0, 0, len(self._lines), len(self._headers)-1,
                                            {'header_row': True, 'data': xls_data, 'columns': xls_headers}
                                            )

        xls_workbook.close()


class CsvExcelToObject:

    def __init__(self, filename: str, expected_headers=None, csv_delimiter=',', csv_quotechar='"', strict_headers=False):

        self._detected_headers = []
        self._header_index_to_name = []
        self._raw_lines = []
        self._objects = []

        if strict_headers and expected_headers is None:
            pylo.PyloEx("CSV/Excel file cannot use strict_headers mode without specifying expected_headers")

        if not os.path.exists(filename):
            raise pylo.PyloEx("File '{}' does not exist".format(filename))

        optional_headers = []
        mandatory_headers_dict = {}
        for header_infos in expected_headers:
            value = header_infos.get('optional')
            if value is None or value is True:
                optional_headers.append(header_infos)
            else:
                mandatory_headers_dict[header_infos['name']] = header_infos

        file_base, file_extension = os.path.splitext(filename)
        file_extension = file_extension.lower()

        if file_extension == '.csv':
            with open(filename) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=csv_delimiter, quotechar=csv_quotechar)
                row_count = 0
                for row in csv_reader:

                    # this is Headers
                    if row_count == 0:
                        item_count = 0
                        for item in row:
                            if item is None or len(item) < 1:
                                raise pylo.PyloEx('CSV headers has blank fields, this is not supported')
                            self._detected_headers.append(item.lower())
                            if strict_headers and item.lower() not in mandatory_headers_dict:
                                raise pylo.PyloEx("CSV/Excel headers have an unexpected header named '{}'".format(item))

                        missing_headers = mandatory_headers_dict.copy()
                        for header_name in self._detected_headers:
                            self._header_index_to_name.append(header_name)
                            if header_name in missing_headers:
                                del missing_headers[header_name]
                        if len(missing_headers) > 0:
                            raise pylo.PyloEx('CSV is missing the following mandatory headers: {}'.format(pylo.string_list_to_text(missing_headers.keys())))

                    # this is DATA
                    else:
                        if len(self._detected_headers) != len(row):
                            raise pylo.PyloEx('CSV line #{} doesnt have the same fields ({}) count than the headers ({}), is it empty?'.format(row_count+1,
                                                                                                                            len(self._detected_headers),
                                                                                                                     len(row)))

                        self._raw_lines.append(row)
                        new_object = {'*line*': row_count+1}
                        self._objects.append(new_object)
                        row_index = 0
                        for item in row:
                            new_object[self._detected_headers[row_index]] = item
                            row_index += 1

                        # handling missing optional columns
                        for opt_header in optional_headers:
                            if opt_header['name'] not in new_object:
                                if 'default' in opt_header:
                                    new_object[opt_header['name']] = opt_header['default']
                                else:
                                    new_object[opt_header['name']] = ''

                    row_count += 1
        elif file_extension == '.xlsx':
            workbook = openpyxl.load_workbook(filename, read_only=True)
            # print("workbook has {} worksheets".format(len(workbook.worksheets)))
            if len(workbook.worksheets) < 1:
                raise pylo.PyloEx("Excel file has no Worksheet")
            source_worksheet = workbook.worksheets[0]

            # print("\n\nmax_col {} max_row {}\n\n".format(source_worksheet.max_column,source_worksheet.max_row))

            for row_count in range(1, source_worksheet.max_row+1):
                # this is Headers
                if row_count == 1:
                    for col_index in range(1, source_worksheet.max_column+1):
                        item = source_worksheet.cell(row_count, col_index).value
                        if item is None or len(item) < 1:
                            raise pylo.PyloEx('Excel headers has blank fields, this is not supported')
                        self._detected_headers.append(item.lower())
                        if strict_headers and item.lower() not in mandatory_headers_dict:
                            raise pylo.PyloEx("CSV/Excel headers have an unexpected header named '{}'".format(item))

                    missing_headers = mandatory_headers_dict.copy()
                    for header_name in self._detected_headers:
                        self._header_index_to_name.append(header_name)
                        if header_name in missing_headers:
                            del missing_headers[header_name]
                    if len(missing_headers) > 0:
                        raise pylo.PyloEx('Excel file is missing the following mandatory headers: {}'.format(pylo.string_list_to_text(missing_headers.keys())))

                # This is DATA
                else:
                    new_object = {'*line*': row_count}
                    self._objects.append(new_object)

                    for col_index in range(1, source_worksheet.max_column+1):
                        item = source_worksheet.cell(row_count, col_index).value
                        new_object[self._detected_headers[col_index-1]] = item

                    # handling missing optional columns
                    for opt_header in optional_headers:
                        if opt_header['name'] not in new_object:
                            if 'default' in opt_header:
                                new_object[opt_header['name']] = opt_header['default']
                            else:
                                new_object[opt_header['name']] = ''

        else:
            raise pylo.PyloEx("Unsupported file extension '{}' in filename {}".format(file_extension, filename))


    def count_lines(self):
        return len(self._objects)

    def count_columns(self):
        return len(self._detected_headers)

    def objects(self):
        return list(self._objects)

    def save_to_csv(self, filename: str, fields_filter: list):
        headers = []
        for field in fields_filter:
            headers.append(field['name'])

        exporter = ArrayToExport(headers)

        for obj in self._objects:
            row = []
            for header in headers:
                row.append(obj.get(header))
            exporter.add_line_from_list(row)

        exporter.write_to_csv(filename)

    def save_to_excel(self, filename: str, fields_filter: list):
        headers = []
        for field in fields_filter:
            headers.append(field['name'])

        exporter = ArrayToExport(headers)

        for obj in self._objects:
            row = []
            for header in headers:
                row.append(obj.get(header))
            exporter.add_line_from_list(row)

        exporter.write_to_excel(filename)

