import argparse
import sys
import logging

from config import create_config_file, ConfigFile
from sorters.seismic_sorter import Sorter


def check_python_version(func):
    def wrapper(*args, **kwargs):
        python_info = sys.version_info
        if python_info.major < 3:
            logging.error('Need python version 3')
            return
        if python_info.minor < 8:
            logging.error('Need python version >=3.8')
            return
        func(*args, **kwargs)
    return wrapper


@check_python_version
def main():
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
            logging.error('Не указан файл конфигурации')
            return
        if args.sort_seis:
            conf = ConfigFile(args.config)
            sorter = Sorter(conf)
            sorter.run()


if __name__ == '__main__':
    main()
