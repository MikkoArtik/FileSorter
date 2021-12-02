import os
import logging

from seiscore import BinaryFile
from seiscore.binaryfile.binaryfile import BadHeaderData

from gravic_files import DATFile, TSFile, ChainFile

from dbase import SqliteDbase
from config import ConfigFile


class Loader:
    def __init__(self, config_file: str):
        if not os.path.exists(config_file):
            raise OSError

        self.config_file = ConfigFile(config_file)
        self.dbase = SqliteDbase(self.config_file.export_root)
        self.gravimetric_root = self.config_file.gravimetric_root
        self.seismic_root = self.config_file.seismic_root
        self.logger = logging.getLogger('Loader')

    def load_chain_files(self):
        self.logger.debug('Loading chains...')
        for root, _, files in os.walk(self.gravimetric_root):
            for filename in files:
                path = os.path.join(root, filename)
                self.logger.debug(f'Start loading file {path}...')
                try:
                    chain_file = ChainFile(path)
                except (OSError, RuntimeError):
                    self.logger.debug(f'File {path} skipped')
                    continue

                chain_id = self.dbase.add_chain(chain_file.sensor_part_name,
                                                path)
                for link_file, link_id in chain_file.links.items():
                    self.dbase.add_link(chain_id, link_id, link_file)
                self.logger.debug(f'Chain info from file {path} added')
        self.logger.debug('Loading chains finished')

    def load_gravity_measures(self, dat_file: DATFile):
        id_val = self.dbase.get_id_dat_file_by_path(dat_file.path)
        if not id_val:
            return
        for measure in dat_file.measures:
            self.dbase.add_gravity_measure(id_val, measure.datetime_val,
                                           measure.corr_grav_value)

    def load_dat_files(self):
        self.logger.debug('Loading dat-files...')
        for root, _, files in os.walk(self.gravimetric_root):
            for filename in files:
                path = os.path.join(root, filename)
                self.logger.debug(f'Start loading file {path}...')
                try:
                    dat_file = DATFile(path)
                except OSError:
                    self.logger.debug(f'File {path} skipped')
                    continue

                if not dat_file.is_good_measures_data:
                    self.logger.error(
                        f'File {filename} incorrect time domain. Skipped')
                    continue

                self.dbase.add_dat_file(dat_file.device_full_number,
                                        dat_file.station,
                                        dat_file.datetime_start,
                                        dat_file.datetime_stop, path)

                self.load_gravity_measures(dat_file)
                self.logger.debug(f'DAT-file {path} added')
        self.logger.debug('Loading dat-files finished')

    def load_tsf_files(self):
        self.logger.debug('Loading tsf-files...')
        for root, _, files in os.walk(self.gravimetric_root):
            for filename in files:
                path = os.path.join(root, filename)
                self.logger.debug(f'Start loading file {path}...')
                try:
                    tsf_file = TSFile(path)
                except OSError:
                    continue
                self.dbase.add_tsf_file(tsf_file.device_num_part,
                                        tsf_file.datetime_start,
                                        tsf_file.datetime_stop, path)
                self.logger.debug(f'TSF-file {path} added')
        self.logger.debug('Loading tsf-files finished')

    def load_seismic_files(self):
        self.logger.debug('Loading seismic files...')
        for root, _, files in os.walk(self.seismic_root):
            for filename in files:
                if not self.config_file.is_seismic_file(filename):
                    self.logger.debug(f'File {filename} is not seismic. '
                                      'Skipped')
                    continue
                path = os.path.join(root, filename)
                self.logger.debug(f'Start loading file {path}...')
                attrs = self.config_file.get_seismic_file_attr(filename)
                station, sensor = attrs.point, attrs.sensor

                bin_data = BinaryFile(path)
                try:
                    dt_start = bin_data.datetime_start
                    dt_stop = bin_data.datetime_stop
                except BadHeaderData:
                    self.logger.error(f'Bad header for file {path}. Skipped')
                    continue

                self.dbase.add_seis_file(sensor, station, dt_start, dt_stop,
                                         path)
                self.logger.debug(f'Seismic file {path} added')
        self.logger.debug('Loading seismic files finished')

    def run(self):
        self.load_chain_files()
        self.load_dat_files()
        self.load_tsf_files()
        self.load_seismic_files()
