import os
from datetime import datetime
from datetime import timedelta
from typing import List, Tuple
import logging

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy

from config import ConfigFile
from dbase import SqliteDbase


EXPORT_CORRECTIONS_FOLDER_NAME = 'corrections'


def get_correction_value(grav_ampl: float, energy_ratio: float) -> float:
    return round(grav_ampl * energy_ratio ** 0.5, 4)


def format_correction_filename(
        datetime_val: datetime, gravimeter_short_number: str,
        seismometer_number: str) -> str:
    datetime_fmt = datetime_val.strftime('%Y%m%d')
    return f'{datetime_fmt}_{gravimeter_short_number}_' \
           f'{seismometer_number}.txt'


class Processing:
    def __init__(self, config_file_path: str):
        if not os.path.exists(config_file_path):
            raise OSError

        self.config = ConfigFile(config_file_path)
        self.db = SqliteDbase(self.config.export_root)
        self.logger = logging.getLogger('Processing')

    @property
    def export_corrections_folder(self) -> str:
        return os.path.join(
            self.config.export_root, EXPORT_CORRECTIONS_FOLDER_NAME)

    def get_energies(self, seis_file_path: str, datetime_min: datetime,
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

    def save_energies(self):
        records = self.db.get_time_intersections()
        for i, record in enumerate(records):
            pair_id, seis_file_id = record[0], record[2]
            min_datetime, max_datetime = record[3:5]
            filepath = self.db.get_seis_file_path_by_id(seis_file_id)
            energies = self.get_energies(filepath, min_datetime,
                                         max_datetime)
            self.db.add_energies(pair_id, energies)
            self.logger.debug(f'Remain - {len(records) - i - 1} files')

    def add_corrections(self):
        self.db.clear_corrections()
        for record in self.db.get_pre_correction_data():
            minute_id, amplitude, energy_ratio = record
            correction_value = get_correction_value(amplitude, energy_ratio)
            self.db.add_single_correction(minute_id, correction_value)

    def get_link_corrections(
            self, chain_id: int, link_id: int, gravimeter_id: int,
            seismometer_id: int) -> List[Tuple[int, int, int, float]]:
        is_pair_exists = self.db.is_sensor_pair_exists(
            chain_id, link_id, gravimeter_id, seismometer_id)

        session_index = self.db.get_session_index(chain_id, link_id)

        corrections_list = []
        if not is_pair_exists:
            measures_count = self.db.get_measures_count_by_link_id(link_id)
            for i in range(measures_count):
                corrections_list.append((session_index, i + 1, 0, 0))
        else:
            records = self.db.get_post_corrections_by_params(
                chain_id, link_id, gravimeter_id, seismometer_id)
            for cycle_index, correction in records:
                if correction is None:
                    correction_value = 0
                    is_bad = 1
                else:
                    correction_value = correction
                    is_bad = 0
                corrections_list.append(
                    (session_index, cycle_index, is_bad, correction_value))
        return corrections_list

    def get_chain_corrections(
            self, chain_id: int, gravimeter_id: int,
            seismometer_id: int) -> List[Tuple[int, int, int, float]]:
        if not self.db.is_chain_has_corrections(
                chain_id, gravimeter_id, seismometer_id):
            return []

        link_ids = self.db.get_links_by_chain_id(chain_id)
        corrections_list = []
        for link_id in link_ids:
            link_corrections = self.get_link_corrections(
                chain_id, link_id, gravimeter_id, seismometer_id)
            corrections_list += link_corrections
        return corrections_list

    def create_export_corrections_folder(self):
        path = os.path.join(self.export_corrections_folder)
        if os.path.exists(path):
            return
        os.makedirs(path)

    def save_corrections(
            self, filename: str,
            corrections: List[Tuple[int, int, int, float]]):
        self.create_export_corrections_folder()

        path = os.path.join(self.export_corrections_folder, filename)
        header = ['seans', 'cycle', 'zabrak', 'popravka']
        with open(path, 'w') as file_ctx:
            file_ctx.write('\t'.join(header) + '\n')
            for record in corrections:
                line = '\t'.join((str(x) for x in record)) + '\n'
                file_ctx.write(line)

    def export_corrections(self):
        chains_ids = self.db.get_all_chain_ids()
        for chain_id_val in chains_ids:
            sensor_pairs = self.db.get_device_pairs_by_chain_id(chain_id_val)
            for gravimeter_id, seismometer_id in sensor_pairs:
                chain_corrections = self.get_chain_corrections(
                    chain_id_val, gravimeter_id, seismometer_id
                )

                if not chain_corrections:
                    continue

                chain_datetime = self.db.get_chain_datetime_by_id(
                    chain_id_val)

                gravimeter_short_number = \
                    self.db.get_gravimeter_short_number_by_id(gravimeter_id)

                seismometer_number = \
                    self.db.get_seismometer_number_by_id(seismometer_id)

                correction_filename = format_correction_filename(
                    chain_datetime, gravimeter_short_number,
                    seismometer_number
                )

                self.save_corrections(correction_filename, chain_corrections)
