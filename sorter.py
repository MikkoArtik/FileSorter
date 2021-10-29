import argparse

from config import create_config_file, ConfigFile
from sorters.seismic_sorter import Sorter

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Утилита для создания структуры папок и сортировка '
                    'файлов записей гравиметров и сейсмометров для '
                    'подготовки проекта совместной обработки '
                    'гравиметрических и сейсмометрических данных'
    )
    parser.add_argument('--create_conf', type=str,
                        help='Путь папки сохранения шаблона '
                             'конфигурационного файла')
    parser.add_argument('--config', type=str,
                        help='Путь к config-файлу')
    parser.add_argument('--sort_seis', type=str, default=True,
                        help='Флаг сортировки сейсмических файлов')

    args = parser.parse_args()

    if args.create_conf:
        create_config_file(args.config)
    else:
        if not args.config:
            raise Exception('Не указан файл конфигурации')
        if args.sort_seis:
            conf = ConfigFile(args.config)
            sorter = Sorter(conf)
            sorter.run()
