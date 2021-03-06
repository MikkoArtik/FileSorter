from datetime import datetime
from datetime import timedelta
import logging
import os
from typing import List, Tuple, NamedTuple

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.pyplot import Figure, Axes
import matplotlib.patches as patches

from seiscore import BinaryFile

from config import ConfigFile
from dbase import SqliteDbase
from gravic_files import TSFile


EXPORT_GRAPHICS_FOLDER_NAME = 'graphics'


def read_z_signal(path: str) -> List[Tuple[datetime, float]]:
    bin_data = BinaryFile(path, use_avg_values=True)
    signal = bin_data.read_signal('Z')
    z_signal = []
    for i, amplitude in enumerate(signal):
        diff_time = timedelta(seconds=i / bin_data.resample_frequency)
        dt_val = bin_data.datetime_start + diff_time
        z_signal.append((dt_val, amplitude))
    return z_signal


class GravityData(NamedTuple):
    quite_level: float
    src_minutes_measures: List[Tuple[datetime, float, bool]]
    src_seconds_measures: List[Tuple[datetime, float]]
    corr_minutes_measures: List[Tuple[datetime, float]]


class SeismicData(NamedTuple):
    quite_level: float
    seis_energy: List[Tuple[datetime, float]]
    src_z_signal: List[Tuple[datetime, float]]


class Plot:
    def __init__(self, title: str, grav_data: GravityData,
                 seis_data: SeismicData, quite_minute_start: datetime,
                 export_path: str):
        self.title = title
        self.__fig, self.__axs = self.prepare()
        self.grav_data = grav_data
        self.seis_data = seis_data
        self.quite_minute_start = quite_minute_start
        self.export_path = export_path

    @property
    def figure(self) -> Figure:
        return self.__fig

    @property
    def subplots(self) -> Axes:
        return self.__axs

    def prepare(self):
        plt.switch_backend('SVG')

        fig, axs = plt.subplots(4, 1)
        fig.set_size_inches(12, 16)
        fig.dpi = 150
        fig.tight_layout(pad=6)

        fig.suptitle(self.title, fontname='Times New Roman', fontsize=16)
        return fig, axs

    def add_src_grav_minutes_measures(self):
        subplot = self.subplots[0]
        subplot.set_title('?????????????? ???????????? ???????????????????????????????? ????????????',
                          fontname='Times New Roman')
        x = [i for i in range(len(self.grav_data.src_minutes_measures))]

        src_measures = [x[1] for x in self.grav_data.src_minutes_measures]
        subplot.plot(
            x, src_measures, color='cornflowerblue',
            alpha=1, linewidth=1.5,
            label='???????????????? ???????????????? ???????? ??????????????, ????????')

    def add_corr_grav_minute_measures(self):
        subplot = self.subplots[0]
        x = [i for i in range(len(self.grav_data.src_minutes_measures))]
        corr_measures = [x[1] for x in self.grav_data.corr_minutes_measures]
        subplot.plot(
            x, corr_measures, color='lightcoral', alpha=1,
            label='???????????????????????? ???????????????? ???????? ??????????????, ????????', linewidth=1.5)

    def create_gravity_curve_filling(self):
        subplot = self.subplots[0]
        x = [i for i in range(len(self.grav_data.src_minutes_measures))]
        src_measures = [x[1] for x in self.grav_data.src_minutes_measures]
        corr_measures = [x[1] for x in self.grav_data.corr_minutes_measures]

        subplot.fill_between(x, src_measures, corr_measures,
                             alpha=0.1, color='green')

    def add_gravity_quite_level(self):
        subplot = self.subplots[0]
        x_limits = 0, len(self.grav_data.src_minutes_measures) - 1
        level_line = [x_limits, [self.grav_data.quite_level] * 2]

        subplot.plot(*level_line, linestyle='dashed', color='blueviolet',
                     alpha=0.5, linewidth=1.5,
                     label='?????????????????????? ?????????? ??????????????')

    def add_gravity_defect_info(self):
        bad_x_scatter, bad_y_scatter = [], []
        good_x_scatter, good_y_scatter = [], []
        for i in range(len(self.grav_data.src_minutes_measures)):
            grav_value = self.grav_data.src_minutes_measures[i][1]
            is_bad = self.grav_data.src_minutes_measures[i][2]
            if is_bad:
                bad_x_scatter.append(i)
                bad_y_scatter.append(grav_value)
            else:
                good_x_scatter.append(i)
                good_y_scatter.append(grav_value)

        subplot = self.subplots[0]
        subplot.scatter(bad_x_scatter, bad_y_scatter, marker='^',
                        color='red', alpha=1, s=20,
                        label='?????????????????????????? ??????????????????')
        subplot.scatter(good_x_scatter, good_y_scatter,
                        marker='^', color='blue', alpha=1, s=20,
                        label='???????????????? ?????????????????? ?? ??????????????????')

    def format_plot_gravity_minutes_measures(self):
        subplot = self.subplots[0]
        subplot.set_title('?????????????? ???????????? ???????????????????????????????? ????????????',
                          fontname='Times New Roman')
        subplot.grid(which='major', color='k', alpha=0.2)

        x_limits = 0, len(self.grav_data.src_minutes_measures) - 1
        subplot.set_xlim(*x_limits)

        subplot.xaxis.set_major_locator(ticker.MultipleLocator(1))
        subplot.set_xlabel('???????????? ???????????? ??????????????????',
                           fontname='Times New Roman')
        subplot.legend(prop={'family': 'Times New Roman'}, loc='center',
                       bbox_to_anchor=(0.5, -0.25), shadow=False, ncol=4)

        subplot.set_yticklabels(
            ['{:.4f}'.format(x) for x in subplot.get_yticks()],
            fontname='Times New Roman')
        subplot.set_ylabel('???????? ??????????????, ????????', fontname='Times New Roman')

    def plot_grav_minutes_data(self):
        self.add_src_grav_minutes_measures()
        self.add_corr_grav_minute_measures()
        self.create_gravity_curve_filling()

        self.add_gravity_quite_level()
        self.add_gravity_defect_info()

        self.format_plot_gravity_minutes_measures()

    def get_seismic_energy_x_axis(self) -> List[int]:
        x_ticks = []
        for item in self.seis_data.seis_energy:
            datetime_val = item[0]
            diff_time = (datetime_val - self.grav_data.src_minutes_measures[0][0])
            minute_index = int(diff_time.total_seconds() / 60)
            x_ticks.append(minute_index)
        return x_ticks

    def get_src_gravity_x_axis(self) -> List[float]:
        x_ticks = []
        dt_start = self.grav_data.src_seconds_measures[0][0]
        for item in self.grav_data.src_seconds_measures:
            datetime_val = item[0]
            diff_time = (datetime_val - dt_start)
            minute_index = diff_time.total_seconds()
            x_ticks.append(minute_index)
        return x_ticks

    def get_src_seismic_x_axis(self) -> List[float]:
        x_ticks = []
        dt_start = self.grav_data.src_seconds_measures[0][0]
        for item in self.seis_data.src_z_signal:
            datetime_val = item[0]
            diff_time = (datetime_val - dt_start)
            minute_index = diff_time.total_seconds()
            x_ticks.append(minute_index)
        return x_ticks

    def plot_seismic_energy(self):
        subplot = self.subplots[1]
        subplot.set_title(
            '???????????????????????????? ???????????????????????????? ???????????????????????? ?????????????????? '
            '(Z-????????????????????)', fontname='Times New Roman')

        x = self.get_seismic_energy_x_axis()
        energies = [x[1] for x in self.seis_data.seis_energy]
        subplot.plot(x, energies, color='mediumblue',
                    alpha=1, linewidth=1.5,
                    label='???????????????????? ???????????????? ??????????????')

        seis_level_line = [[x[0], x[-1]], [self.seis_data.quite_level] * 2]
        subplot.plot(*seis_level_line, color='seagreen', alpha=0.5,
                     linewidth=1.5, label='?????????????????????? ?????????? ??????????????',
                     linestyle='dashed')

        subplot.fill_between(x, energies,
                             [self.seis_data.quite_level] * len(x),
                             alpha=0.1, color='blue')

        subplot.sharex(self.subplots[0])
        subplot.grid(which='major', color='k', alpha=0.2)
        subplot.xaxis.set_major_locator(ticker.MultipleLocator(1))
        subplot.set_xlabel('???????????? ???????????? ??????????????????',
                           fontname='Times New Roman')
        subplot.set_yticklabels([str(int(x)) for x in subplot.get_yticks()],
                                fontname='Times New Roman')
        subplot.set_ylabel('??????????????, ??????. ????', fontname='Times New Roman')

        subplot.legend(prop={'family': 'Times New Roman'}, loc='center',
                       bbox_to_anchor=(0.5, -0.2), shadow=False, ncol=2)

    def plot_quite_minute(self):
        dt_start = self.grav_data.src_seconds_measures[0][0]
        x_start = (self.quite_minute_start - dt_start).total_seconds()

        subplot = self.subplots[2]
        grav_ampls = [x[1] for x in self.grav_data.src_seconds_measures]
        amp_min, amp_max = min(grav_ampls), max(grav_ampls)
        rectangle = patches.Rectangle(
            (x_start, amp_min), 60, amp_max - amp_min,
            color='seagreen', alpha=0.5, label='?????????? ?????????????? ??????????????')
        subplot.add_patch(rectangle)

        subplot = self.subplots[3]
        seis_ampls = [x[1] for x in self.seis_data.src_z_signal]
        amp_min, amp_max = min(seis_ampls), max(seis_ampls)
        rectangle = patches.Rectangle(
            (x_start, amp_min), 60, amp_max - amp_min,
            color='seagreen', alpha=0.5, label='?????????? ?????????????? ??????????????')
        subplot.add_patch(rectangle)

    def plot_src_gravity_signal(self):
        subplot = self.subplots[2]
        subplot.set_title('???????????????? ???????????????????????????????? ????????????',
                          fontname='Times New Roman')

        x = self.get_src_gravity_x_axis()
        amplitudes = [x[1] for x in self.grav_data.src_seconds_measures]
        subplot.plot(x, amplitudes, color='crimson',
                     alpha=1, linewidth=1,
                     label='???????????????? ???????????????????????????????? ????????????')

        subplot.grid(which='major', color='k', alpha=0.2)
        subplot.set_xlim(x[0], x[-1])
        subplot.xaxis.set_major_locator(ticker.MultipleLocator(60))
        subplot.set_xlabel('??????????, ??', fontname='Times New Roman')
        subplot.set_ylabel('??????????????????, ??????. ????', fontname='Times New Roman')

        subplot.legend(prop={'family': 'Times New Roman'}, loc='center',
                       bbox_to_anchor=(0.5, -0.2), shadow=False, ncol=2)

    def plot_src_seismic_signal(self):
        subplot = self.subplots[3]
        subplot.set_title('???????????????? ???????????????????????? ???????????? (Z-????????????????????)',
                          fontname='Times New Roman')

        x = self.get_src_seismic_x_axis()
        amplitudes = [x[1] for x in self.seis_data.src_z_signal]
        subplot.plot(x, amplitudes, color='cornflowerblue',
                     alpha=1, linewidth=1,
                     label='???????????????? ???????????????????????? ???????????? (Z-????????????????????)')
        subplot.sharex(self.subplots[2])

        subplot.grid(which='major', color='k', alpha=0.2)
        subplot.xaxis.set_major_locator(ticker.MultipleLocator(60))
        subplot.set_xlabel('??????????, ??', fontname='Times New Roman')
        subplot.set_ylabel('??????????????????, ??????. ????', fontname='Times New Roman')
        subplot.legend(prop={'family': 'Times New Roman'}, loc='center',
                       bbox_to_anchor=(0.5, -0.2), shadow=False, ncol=2)

    def run(self):
        self.plot_grav_minutes_data()
        self.plot_seismic_energy()
        self.plot_src_gravity_signal()
        self.plot_src_seismic_signal()
        plt.savefig(self.export_path, dpi=150)

        self.figure.clear()
        plt.close(self.figure)


class Plotting:
    def __init__(self, config_file_path: str):
        if not os.path.exists(config_file_path):
            raise OSError

        self.config = ConfigFile(config_file_path)
        self.dbase = SqliteDbase(self.config.export_root)
        self.logger = logging.getLogger('Plotting')
        self.__create_export_folder()

    @property
    def export_folder_path(self) -> str:
        return os.path.join(self.config.export_root,
                            EXPORT_GRAPHICS_FOLDER_NAME)

    def __create_export_folder(self):
        if not os.path.exists(self.export_folder_path):
            os.makedirs(self.export_folder_path)

    def generate_filename(self, measure_pair_id: int) -> str:
        info = self.dbase.get_sensor_pair_info(measure_pair_id)
        return '_'.join(info).replace('.', '-') + '.png'

    def generate_title(self, ti_id: int) -> str:
        info = self.dbase.get_sensor_pair_info(ti_id)
        title = f'?????????????????? ?????????? ???????????????? (?????????? ??? {info[0]} ?????????????????? ' \
                f'{info[1]} ???????????????????? {info[2]} ???????? {info[3]})'
        return title

    def create_plot(self, measure_pair_id: int):
        src_grav_m = self.dbase.get_grav_minute_measures(measure_pair_id)
        corr_val = self.dbase.get_seis_corrections(measure_pair_id)
        corr_grav_m = [(x[0], x[1] + corr_val[i]) for i, x in
                       enumerate(src_grav_m)]
        quite_level = self.dbase.get_grav_level(measure_pair_id)
        if not quite_level:
            return

        tsf_file_path = self.dbase.get_tsf_file_path(measure_pair_id)
        tsf_data = TSFile(tsf_file_path)
        src_seconds_measures = tsf_data.src_signal

        grav_data = GravityData(quite_level, src_grav_m, src_seconds_measures,
                                corr_grav_m)

        seis_energy = self.dbase.get_seis_energy(measure_pair_id)
        seis_level = self.dbase.get_seis_level(measure_pair_id)

        seis_file_path = self.dbase.get_seis_file_path(measure_pair_id)
        z_signal = read_z_signal(seis_file_path)

        seis_data = SeismicData(seis_level, seis_energy, z_signal)

        quite_minute = self.dbase.get_quite_minute_start(measure_pair_id)

        title = self.generate_title(measure_pair_id)
        filename = self.generate_filename(measure_pair_id)

        export_path = os.path.join(self.export_folder_path, filename)

        plotter = Plot(title, grav_data, seis_data, quite_minute, export_path)
        plotter.run()

    def run(self):
        measure_pairs_ids = [x[0] for x in self.dbase.get_measure_pairs()]
        for ti_id in measure_pairs_ids:
            self.create_plot(ti_id)
