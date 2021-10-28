import argparse
import os
from typing import List, NamedTuple

from loguru import logger


SEIS_FILE_FORMATS = {'00', 'xx', 'bin'}


class FileAttr(NamedTuple):
    name: str

    @property
    def order(self) -> int:
        return int(self.name.split('_')[0])

    @property
    def point_number(self) -> int:
        return int(self.name.split('_')[1])

    @property
    def date(self) -> str:
        return self.name.split('_')[3]


class Sorter:
    def __init__(self, root: str):
        if not os.path.exists(root):
            logger.error(f'Invalid root folder - {root}')
            raise OSError
        self.root = root

    def get_all_seismic_files(self) -> List[str]:
        filenames = []
        for filename in os.listdir(self.root):
            file_format = filename.split('.')[-1]
            if file_format in SEIS_FILE_FORMATS:
                filenames.append(filename)
        return filenames

    def create_folder_by_filename(self, filename: str) -> str:
        attrs = FileAttr(filename)
        child_folder_name = f'{attrs.order}_{attrs.point_number}'
        folder_path = os.path.join(self.root, attrs.date, child_folder_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.debug(f'Folder for file {filename} created')
        return folder_path

    def run(self):
        for filename in self.get_all_seismic_files():
            logger.debug(f'Start processing for file {filename}...')
            origin_file_path = os.path.join(self.root, filename)
            new_file_path = os.path.join(
                self.create_folder_by_filename(filename), filename)
            os.rename(origin_file_path, new_file_path)
            logger.debug(f'File {filename} moved')
        logger.debug('Sorting done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Утилита создания структуры папок для '
                    'совместной обработки гравики и микросейсмики')
    parser.add_argument('root', type=str,
                        help='Корневая папка с сейсмическими файлами')
    args = parser.parse_args()
    Sorter(args.root).run()
