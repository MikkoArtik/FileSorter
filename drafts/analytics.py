import os

from gravity_report import Extractor


if __name__ == '__main__':
    stations = ['1081', '1126', '1221', '1333', '1343', '5014', '5015',
                '5023', '5027', '5037', '5039', '5040', '10262', '11112',
                '11512', '12342', '44132', '50182', '60762', '70211']
    dates = ['2021-09-12', '2021-09-06', '2021-09-07']

    root = '/media/michael/Data/TEMP/reports'
    export_folder = '/media/michael/Data/TEMP/reports'

    for root_folder, _, files in os.walk(root):
        for filename in files:
            extension = filename.split('.')[-1]
            if extension.lower() != 'xls':
                continue

            xls_path = os.path.join(root_folder, filename)
            extractor = Extractor(xls_path, export_folder, stations, dates)
            extractor.run()
