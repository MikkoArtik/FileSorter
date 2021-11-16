import os
from datetime import datetime
from datetime import timedelta
from typing import List
import logging

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy

from config import ConfigFile
from dbase import SqliteDbase


class Processing:
    def __init__(self, config_file_path: str):
        if not os.path.exists(config_file_path):
            raise OSError

        self.config = ConfigFile(config_file_path)
        self.db = SqliteDbase(self.config.export_root)
        self.logger = logging.getLogger('Processing')

    def get_origin_energies(self, seis_file_path: str, datetime_min: datetime,
                            datetime_max: datetime,
                            split_seconds=60) -> List[List[float]]:
        self.logger.debug(f'Starting energy calculation for {seis_file_path}')
        intervals_count = int(
            (datetime_max - datetime_min).total_seconds() / split_seconds)

        bin_data = BinaryFile(seis_file_path, use_avg_values=True)
        f_min, f_max = self.config.get_bandpass_freqs()

        energies = []
        components = ['X', 'Y', 'Z']
        for i in range(intervals_count):
            left_datetime = datetime_min + timedelta(seconds=split_seconds * i)
            right_datetime = left_datetime + timedelta(seconds=split_seconds)

            bin_data.read_date_time_start = left_datetime
            bin_data.read_date_time_stop = right_datetime

            component_energy = []
            for component in components:
                signal = bin_data.read_signal(component)
                spectrum_data = spectrum(signal, bin_data.resample_frequency)
                energy_val = spectrum_energy(spectrum_data, (f_min, f_max))
                component_energy.append(energy_val)
            full_energy = (sum((x ** 2 for x in component_energy))) ** 0.5
            all_energies = component_energy + [full_energy]
            energies.append(all_energies)

        self.logger.debug(f'Energy calculation for {seis_file_path} finished')
        return energies

    def save_origin_energies(self):
        records = self.db.get_time_intersections()
        for i, record in enumerate(records):
            pair_id, seis_file_id = record[0], record[2]
            min_datetime, max_datetime = record[3:5]
            filepath = self.db.get_seis_file_path_by_id(seis_file_id)
            energies = self.get_origin_energies(filepath, min_datetime,
                                                max_datetime)
            self.db.add_energies(pair_id, energies)
            self.logger.debug(f'Remain - {len(records) - i - 1} files')
