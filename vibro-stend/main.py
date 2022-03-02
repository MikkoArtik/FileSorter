import os

from processing import Processing


if __name__ == '__main__':
    root = '/media/michael/Data/Projects/GraviSeismicComparation/' \
           '2022_МОСКВА_МФТИ_ИДГ'
    config_files = ['19-01-2022 (K07-1418).yml',
                    '20-01-2022 (K07-458).yml',
                    '20-01-2022 (K07-1418).yml',
                    '21-01-2022 (K07-458).yml']

    for filename in config_files:
        path = os.path.join(root, filename)
        Processing(path).run()
