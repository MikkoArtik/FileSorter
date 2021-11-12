import os

from seiscore import BinaryFile

from sorters.gravic_sorter import DATFile, TSFile
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

    def load_dat_files(self):
        for root, _, files in os.walk(self.gravimetric_root):
            for filename in files:
                path = os.path.join(root, filename)
                try:
                    dat_file = DATFile(path)
                except OSError:
                    continue
                self.dbase.add_dat_file(dat_file.device_full_number,
                                        dat_file.station,
                                        dat_file.datetime_start,
                                        dat_file.datetime_stop, path)

    def load_tsf_files(self):
        for root, _, files in os.walk(self.gravimetric_root):
            for filename in files:
                path = os.path.join(root, filename)
                try:
                    tsf_file = TSFile(path)
                except OSError:
                    continue
                self.dbase.add_tsf_file(tsf_file.device_num_part,
                                        tsf_file.datetime_start,
                                        tsf_file.datetime_stop, path)

    def load_seismic_files(self):
        for root, _, files in os.walk(self.seismic_root):
            for filename in files:
                if not self.config_file.is_seismic_file(filename):
                    continue
                path = os.path.join(root, filename)

                attrs = self.config_file.get_seismic_file_attr(filename)
                station, sensor = attrs.point, attrs.sensor

                bin_data = BinaryFile(path)
                dt_start = bin_data.datetime_start
                dt_stop = bin_data.datetime_stop

                self.dbase.add_seis_file(sensor, station, dt_start, dt_stop,
                                         path)

    def run(self):
        self.load_dat_files()
        self.load_tsf_files()
        self.load_seismic_files()
