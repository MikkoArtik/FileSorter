import os
from datetime import datetime
from typing import List, NamedTuple, Set

import xlrd
from xlrd import open_workbook


EXTRACT_COLUMNS = ['punkt__', 'datareis', 'd_g_finish', 'skp_otobr',
                   'kvo_otobr']


class TableRow(NamedTuple):
    station: str
    date_val: datetime
    gravity_val: float
    std_val: float
    selection_size: int


class ReportXLSTable:
    def __init__(self, path: str):
        if not os.path.exists(path):
            raise OSError(f'Excel file {path} not found')

        self.__workbook = open_workbook(filename=path)
        self.__headers = []
        self.__report_data = self.__extract_data()

    @property
    def workbook(self) -> xlrd.Book:
        return self.__workbook

    @property
    def worksheet(self) -> xlrd.sheet.Sheet:
        return self.workbook.sheets()[0]

    @property
    def headers(self) -> List[int]:
        if not self.__headers:
            headers = []
            for i in range(self.columns_count):
                column_header = self.worksheet.cell(0, i).value
                if column_header == EXTRACT_COLUMNS[len(headers)]:
                    headers.append(i)
                if len(headers) == len(EXTRACT_COLUMNS):
                    break
            self.__headers = headers
        return self.__headers

    @property
    def rows_count(self) -> int:
        return self.worksheet.nrows

    @property
    def columns_count(self) -> int:
        return self.worksheet.ncols

    @property
    def data(self) -> List[TableRow]:
        return self.__report_data

    def __parse_row(self, row_index: int) -> TableRow:
        src_vals = [self.worksheet.cell(row_index, x).value for x in self.headers]
        src_vals[0] = str(int(src_vals[0]))
        src_vals[1] = datetime.strptime(src_vals[1], '%Y-%m-%d')
        return TableRow(*src_vals)

    def __extract_data(self) -> List[TableRow]:
        extractions = []
        for i in range(1, self.rows_count):
            extractions.append(self.__parse_row(i))
        return extractions

    def filter_rows_by_stations(self, rows: List[TableRow],
                                stations: Set[str]) -> List[TableRow]:
        filtered_rows = set()
        for row in rows:
            if row.station in stations:
                filtered_rows.add(row)
        return list(filtered_rows)

    def filter_rows_by_dates(self, rows: List[TableRow],
                             dates: Set[datetime]) -> List[TableRow]:
        filtered_rows = set()
        for row in rows:
            if row.date_val in dates:
                filtered_rows.add(row)
        return list(filtered_rows)


class Extractor:
    def __init__(self, xls_path: str, export_folder: str,
                 station_list: List[str], date_list: List[str]):
        if not os.path.exists(xls_path):
            raise OSError('Invalid xls path')

        if not os.path.exists(export_folder):
            raise OSError('Invalid export folder path')

        self.xls_path = xls_path
        self.export_folder = export_folder
        self.stations = set(station_list)
        self.dates = set((datetime.strptime(x, '%Y-%m-%d') for x in date_list))

    def get_filter_data(self) -> List[TableRow]:
        report = ReportXLSTable(self.xls_path)
        station_filter = report.filter_rows_by_stations(report.data,
                                                        self.stations)
        date_filter = report.filter_rows_by_dates(station_filter, self.dates)
        return date_filter

    def save_to_file(self, rows: List[TableRow]):
        xls_base_name = os.path.basename(self.xls_path).split('.')[0]
        output_file = os.path.join(self.export_folder,
                                   f'extraction_{xls_base_name}.txt')
        with open(output_file, 'w') as file_ctx:
            file_ctx.write('\t'.join(EXTRACT_COLUMNS) + '\n')
            for row in rows:
                tmp = [row.station, row.date_val.strftime('%Y-%m-%d'),
                     str(row.gravity_val), str(row.std_val),
                     str(row.selection_size)]
                file_ctx.write('\t'.join(tmp) + '\n')

    def run(self):
        filter_rows = self.get_filter_data()
        self.save_to_file(filter_rows)
