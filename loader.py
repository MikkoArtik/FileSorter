import os
import logging

from datetime import datetime
from datetime import timedelta

from seiscore import BinaryFile
from seiscore.binaryfile.binaryfile import BadHeaderData

from sorters.gravic_sorter import DATFile, TSFile, ChainFile
from dbase import SqliteDbase
from config import ConfigFile


def get_intersection_time(grav_time: datetime, seis_time: datetime,
                          edge_type: str) -> datetime:
    if seis_time == grav_time:
        return seis_time

    variant_datetime = datetime(seis_time.year, seis_time.month,
                                seis_time.day, seis_time.hour,
                                seis_time.minute, grav_time.second)
    if edge_type == 'left':
        if variant_datetime > seis_time:
            return variant_datetime
        else:
            return variant_datetime + timedelta(minutes=1)
    else:
        if variant_datetime < seis_time:
            return variant_datetime
        else:
            return variant_datetime + timedelta(minutes=-1)


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
        for measure in dat_file.extract_all_measures():
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

    def set_intersection_times(self):
        self.dbase.clear_time_intersections()
        grav_seis_pairs = self.dbase.get_grav_seis_times()
        for grav_id, seis_id, *times in grav_seis_pairs:
            grav_dt_start, grav_dt_stop = times[:2]
            seis_dt_start, seis_dt_stop = times[2:]

            left_limit = get_intersection_time(grav_dt_start, seis_dt_start,
                                               'left')
            right_limit = get_intersection_time(grav_dt_stop, seis_dt_stop,
                                                'right')
            if left_limit < right_limit:
                self.dbase.add_time_intersection(grav_id, seis_id,
                                                 left_limit, right_limit)

    def run(self):
        self.load_chain_files()
        self.load_dat_files()
        self.load_tsf_files()
        self.load_seismic_files()
        self.set_intersection_times()
