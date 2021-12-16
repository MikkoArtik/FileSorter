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


def get_intersection_time(grav_time: datetime, seis_time: datetime,
                          edge_type: str) -> datetime:
    if seis_time == grav_time:
        return seis_time

    result = seis_time + timedelta(
        seconds=grav_time.second - seis_time.second)
    if edge_type == 'left':
        if seis_time < grav_time:
            return grav_time
        if result < seis_time:
            result += timedelta(minutes=1)
    else:
        if seis_time > grav_time:
            return grav_time
        if result > seis_time:
            result += timedelta(minutes=-1)
    return result


def get_correction_value(grav_ampl: float, energy_ratio: float) -> float:
    return round(grav_ampl * (1 - 1 / energy_ratio ** 0.5), 4)


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
        self.dbase = SqliteDbase(self.config.export_root)
        self.logger = logging.getLogger('Processing')

    @property
    def export_corrections_folder(self) -> str:
        return os.path.join(
            self.config.export_root, EXPORT_CORRECTIONS_FOLDER_NAME)

    def set_intersection_times(self):
        self.dbase.clear_grav_seis_time_intersections()
        grav_seis_pairs = self.dbase.get_grav_seis_pairs()
        for grav_id, seis_id, *times in grav_seis_pairs:
            grav_dt_start, grav_dt_stop = times[:2]
            seis_dt_start, seis_dt_stop = times[2:]

            left_limit = get_intersection_time(grav_dt_start, seis_dt_start,
                                               'left')
            right_limit = get_intersection_time(grav_dt_stop, seis_dt_stop,
                                                'right')
            if left_limit < right_limit:
                self.dbase.add_grav_seis_time_intersection(
                    grav_id, seis_id, left_limit, right_limit)

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

    def get_median_energies(
            self, component_energies: List[List[float]]) -> List[float]:
        medians = []
        for i in range(len(component_energies[0])):
            enegry_vals = [x[i] for x in component_energies]
            medians.append(float(np.median(enegry_vals)))
        return medians

    def save_energies(self):
        self.dbase.delete_all_energies()
        records = self.dbase.get_grav_seis_time_intersections()
        for i, record in enumerate(records):
            pair_id, seis_file_id = record[0], record[2]
            min_datetime, max_datetime = record[3:5]
            filepath = self.dbase.get_seis_file_path_by_id(seis_file_id)
            energies = self.get_energies(filepath, min_datetime,
                                         max_datetime)
            median_energies = self.get_median_energies(energies)

            self.dbase.add_energies(pair_id, energies)
            self.dbase.add_median_energies(pair_id, median_energies)

            self.logger.debug(f'Remain - {len(records) - i - 1} files')

    def add_corrections(self):
        self.dbase.clear_corrections()
        corrections = {}
        for record in self.dbase.get_pre_correction_data():
            time_intersection_id, grav_measure_id = record[:2]
            amplitude, energy_ratio = record[2:]
            correction_value = get_correction_value(amplitude, energy_ratio)

            vals = corrections.get(time_intersection_id, [])
            vals.append((grav_measure_id, correction_value))
            corrections[time_intersection_id] = vals

        for ti_id, vals in corrections.items():
            self.dbase.add_seis_corrections(ti_id, vals)

    def get_link_corrections(
            self, chain_id: int, link_id: int, gravimeter_id: int,
            seismometer_id: int) -> List[Tuple[int, int, int, float]]:
        is_pair_exists = self.dbase.is_sensor_pair_exists(
            chain_id, link_id, gravimeter_id, seismometer_id)

        link_index = self.dbase.get_link_index(chain_id, link_id)

        corrections_list = []
        if not is_pair_exists:
            is_bad_info = self.dbase.get_gravity_defect_info_by_link_id(
                link_id)
            for i, is_bad in enumerate(is_bad_info):
                corrections_list.append((link_index, i + 1, is_bad, 0))
        else:
            records = self.dbase.get_post_corrections_by_params(
                chain_id, link_id, gravimeter_id, seismometer_id)
            for cycle_index, is_bad, correction_value in records:
                corrections_list.append(
                    (link_index, cycle_index, is_bad, correction_value))
        return corrections_list

    def get_chain_corrections(
            self, chain_id: int, gravimeter_id: int,
            seismometer_id: int) -> List[Tuple[int, int, int, float]]:
        if not self.dbase.is_chain_has_corrections(
                chain_id, gravimeter_id, seismometer_id):
            return []

        link_ids = self.dbase.get_links_by_chain_id(chain_id)
        corrections_list = []
        for link_id in link_ids:
            link_corrections = self.get_link_corrections(
                chain_id, link_id, gravimeter_id, seismometer_id)
            corrections_list += link_corrections
        return corrections_list

    def save_corrections(self, folder_root: str, filename: str,
            corrections: List[Tuple[int, int, int, float]]):
        if not os.path.exists(folder_root):
            os.makedirs(folder_root)

        path = os.path.join(folder_root, filename)
        header = ['seans', 'cycle', 'zabrak', 'popravka']
        with open(path, 'w') as file_ctx:
            file_ctx.write('\t'.join(header) + '\r\n')
            for record in corrections:
                line = '\t'.join((str(x) for x in record)) + '\r\n'
                file_ctx.write(line)

    def export_corrections(self, chain_ids: List[int] = []):
        if not chain_ids:
            chain_ids = self.dbase.get_all_chain_ids()

        for chain_id_val in chain_ids:
            sensor_pairs = self.dbase.get_device_pairs_by_chain_id(chain_id_val)
            for gravimeter_id, seismometer_id in sensor_pairs:
                chain_corrections = self.get_chain_corrections(
                    chain_id_val, gravimeter_id, seismometer_id
                )

                if not chain_corrections:
                    continue

                chain_datetime = self.dbase.get_chain_datetime_by_id(
                    chain_id_val)

                gravimeter_short_number = \
                    self.dbase.get_gravimeter_short_number_by_id(
                        gravimeter_id)

                seismometer_number = \
                    self.dbase.get_seismometer_number_by_id(seismometer_id)

                export_folder = os.path.join(
                    self.export_corrections_folder,
                    f'{gravimeter_short_number}-{seismometer_number}',
                    chain_datetime.strftime('%Y_%m_%d'),
                    gravimeter_short_number
                )

                correction_filename = self.dbase.get_correction_filename(
                    chain_id_val)
                self.save_corrections(export_folder, correction_filename,
                                      chain_corrections)

    def run(self):
        self.set_intersection_times()
        self.save_energies()
        self.add_corrections()
        self.export_corrections()

    def recalc_corrections(self):
        self.save_energies()
        self.add_corrections()
