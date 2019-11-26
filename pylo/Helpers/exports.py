import pylo.vendors.xlsxwriter as xlsxwriter
import csv
import pylo


class ArrayToExport:
    def __init__(self, headers):
        self._headers = headers
        self._headers = headers
        self._columns = len(headers)
        self._lines = []

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
