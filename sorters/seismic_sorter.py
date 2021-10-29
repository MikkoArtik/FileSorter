import logging
import os
from typing import Dict

from config import ConfigFile


class Sorter:
    def __init__(self, config: ConfigFile):
        self.conf = config

    def get_all_seismic_files(self) -> Dict[str, str]:
        filenames = dict()
        for root, folders, files in os.walk(self.conf.seismic_root):
            for filename in files:
                file_format = filename.split('.')[-1]
                if file_format in self.conf.seismic_extensions:
                    filenames[filename] = os.path.join(root, filename)
        return filenames

    def create_folder_by_filename(self, filename: str) -> str:
        attrs = self.conf.get_seismic_file_attr(filename)
        child_folder_name = f'{attrs.order}_{attrs.point}'
        folder_path = os.path.join(self.conf.export_root, child_folder_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logging.debug(f'Folder for file {filename} created')
        return folder_path

    def run(self):
        for filename, origin_file_path in self.get_all_seismic_files().items():
            logging.debug(f'Start processing for file {filename}...')
            new_file_path = os.path.join(
                self.create_folder_by_filename(filename), filename)
            os.rename(origin_file_path, new_file_path)
            logging.debug(f'File {filename} moved')
        logging.debug('Sorting of seismic files is done')
