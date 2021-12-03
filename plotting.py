from datetime import datetime
from datetime import timedelta
import logging
import os
from typing import List, Tuple, NamedTuple

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.pyplot import Figure, Axes

from seiscore import BinaryFile
from seiscore.functions.filter import band_pass_filter

from config import ConfigFile
from dbase import SqliteDbase
from gravic_files import TSFile


class GravityData(NamedTuple):
    quite_level: float
    minutes_src: List[float]
    minutes_corr: List[float]
    seconds_src: List[Tuple[datetime, int]]


class Plot:
    def __init__(self, title: str, grav_data: GravityData):
        self.title = title
        self.__fig, self.__axs = self.prepare()
        self.grav_data = grav_data

    @property
    def figure(self) -> Figure:
        return self.__fig

    @property
    def subplots(self) -> Axes:
        return self.__axs

    def prepare(self):
        fig, axs = plt.subplots(4, 1)
        fig.set_size_inches(12, 12)
        fig.dpi = 150
        fig.tight_layout(pad=3.0)

        fig.suptitle(self.title, fontname='Times New Roman', fontsize=16)

        return fig, axs

    def plot_grav_minutes_data(self):
        subplot = self.subplots[0]
        subplot.set_title('Сводный график гравиметрических данных',
                          fontname='Times New Roman')
        x = [i for i in range(len(self.grav_data.minutes_src))]

        subplot.plot(
            x, self.grav_data.minutes_src, color='cornflowerblue',
            alpha=1, linewidth=1.5,
            label='Исходное значение силы тяжести, мГал')

        subplot.plot(
            x, self.grav_data.minutes_corr, color='lightcoral', alpha=1,
            label='Исправленное значение силы тяжести, мГал', linewidth=1.5)

        subplot.fill_between(
            x, self.grav_data.minutes_src, self.grav_data.minutes_corr,
            alpha=0.1, color='silver')

        level_line = [(x[0], x[-1]), [self.grav_data.quite_level] * 2]

        subplot.plot(
            *level_line, linestyle='dashed', color='blueviolet', alpha=0.5,
            linewidth=1.5, label='Сейсмически тихий уровень')

        subplot.grid(which='major', color='k', alpha=0.2)
        subplot.set_xlim(x[0], x[-1])
        subplot.xaxis.set_major_locator(ticker.MultipleLocator(1))
        subplot.set_xlabel(
            'Индекс минуты измерений', fontname='Times New Roman')
        subplot.legend(prop={'family': 'Times New Roman'})

        subplot.set_yticklabels(
            ['{:.4f}'.format(x) for x in subplot.get_yticks()],
            fontname='Times New Roman')
        subplot.set_ylabel('Сила тяжести, мГал', fontname='Times New Roman')

    def plot_seismic_energy(self):
        subplot = self.subplots[1]
        subplot.set_title(
            'Энергетическая характеристика сейсмических колебаний '
            '(Z-компонента)', fontname='Times New Roman')
        x = subplot
        subplot.plot(x, seis_energy_z, color='mediumblue',
                    alpha=1, linewidth=1.5,
                    label='Поминутное значение энергии')

        min_z_energy = min(seis_energy_z)
        seis_level_line = [(x[0], x[-1]), [min_z_energy] * 2]
        subplot.plot(*seis_level_line, color='seagreen',
                    alpha=0.5, linewidth=1.5,
                    label='Сейсмически тихий уровень',
                    linestyle='dashed')
        subplot.set_xlim(x[0], x[-1])
        axs[1].grid(which='major', color='k', alpha=0.2)
        axs[1].xaxis.set_major_locator(ticker.MultipleLocator(1))
        axs[1].set_xlabel('Индекс минуты измерений',
                          fontname='Times New Roman')
        axs[1].set_yticklabels([str(int(x)) for x in axs[1].get_yticks()],
                               fontname='Times New Roman')
        axs[1].set_ylabel('Энергия, усл. ед', fontname='Times New Roman')

        axs[1].legend(prop={'family': 'Times New Roman'})


def create_plot(
        origin_grav_measures: List[float],
        corrected_grav_measures: List[float],
        seis_energy_z: List[float], gravity_level: float, title: str,
        export_folder: str):
    fig, axs = plt.subplots(2, 1)
    fig.set_size_inches(12, 6)
    fig.dpi = 150
    fig.tight_layout(pad=3.0)

    fig.suptitle(title, fontname='Times New Roman', fontsize=16)

    x = [i for i in range(len(origin_grav_measures))]
    axs[0].set_title('Сводный график гравиметрических данных',
                     fontname='Times New Roman')
    axs[0].plot(x, origin_grav_measures, color='cornflowerblue', alpha=1,
                linewidth=1.5, label='Исходное значение силы тяжести, мГал')
    axs[0].plot(x, corrected_grav_measures, color='lightcoral', alpha=1,
                label='Исправленное значение силы тяжести, мГал',
                linewidth=1.5)
    axs[0].fill_between(x, origin_grav_measures, corrected_grav_measures,
                        alpha=0.1, color='silver')

    level_line = [(x[0], x[-1]), [gravity_level] * 2]

    axs[0].plot(*level_line, linestyle='dashed', color='blueviolet',
                alpha=0.5, linewidth=1.5, label='Сейсмически тихий уровень')

    axs[0].grid(which='major', color='k', alpha=0.2)
    axs[0].set_xlim(x[0], x[-1])
    axs[0].xaxis.set_major_locator(ticker.MultipleLocator(1))
    axs[0].set_xlabel('Индекс минуты измерений', fontname='Times New Roman')
    axs[0].legend(prop={'family': 'Times New Roman'})

    axs[0].set_yticklabels(
        ['{:.4f}'.format(x) for x in axs[0].get_yticks()],
        fontname='Times New Roman')
    axs[0].set_ylabel('Сила тяжести, мГал', fontname='Times New Roman')

    axs[1].set_title('Энергетическая характеристика сейсмических колебаний '
                     '(Z-компонента)', fontname='Times New Roman')
    axs[1].plot(x, seis_energy_z, color='mediumblue',
                alpha=1, linewidth=1.5, label='Поминутное значение энергии')

    min_z_energy = min(seis_energy_z)
    seis_level_line = [(x[0], x[-1]), [min_z_energy] * 2]
    axs[1].plot(*seis_level_line, color='seagreen',
                alpha=0.5, linewidth=1.5, label='Сейсмически тихий уровень',
                linestyle='dashed')
    axs[1].set_xlim(x[0], x[-1])
    axs[1].grid(which='major', color='k', alpha=0.2)
    axs[1].xaxis.set_major_locator(ticker.MultipleLocator(1))
    axs[1].set_xlabel('Индекс минуты измерений', fontname='Times New Roman')
    axs[1].set_yticklabels([str(int(x)) for x in axs[1].get_yticks()],
                           fontname='Times New Roman')
    axs[1].set_ylabel('Энергия, усл. ед', fontname='Times New Roman')

    axs[1].legend(prop={'family': 'Times New Roman'})

    export_path = os.path.join(export_folder, f'{title}.png')
    # plt.savefig(export_path)
    plt.show()
    # plt.close()


def create_signals_plot(grav_signal: List[Tuple[datetime, float]],
                        seis_signal: List[Tuple[datetime, float]]):
    fig, axs = plt.subplots(nrows=2, ncols=1, sharex='col')
    fig.set_size_inches(12, 6)
    fig.dpi = 150
    fig.tight_layout(pad=3.0)

    grav_x = [x[0] for x in grav_signal]
    grav_val = [x[1] for x in grav_signal]
    axs[0].plot(grav_x, grav_val, color='cornflowerblue', alpha=1,
                linewidth=1.5, label='Исходное значение силы тяжести, мГал')

    seis_x = [x[0] for x in seis_signal]
    seis_vals = [x[1] for x in seis_signal]
    axs[1].plot(seis_x, seis_vals, color='cornflowerblue', alpha=1,
                linewidth=1.5, label='Исходное значение силы тяжести, мГал')
    plt.show()


class Plotting:
    def __init__(self, config_file_path: str):
        if not os.path.exists(config_file_path):
            raise OSError

        self.config = ConfigFile(config_file_path)
        self.dbase = SqliteDbase(self.config.export_root)
        self.logger = logging.getLogger('Plotting')

    def get_tsf_file_signal_by_id(
            self, id_val: int) -> List[Tuple[datetime, int]]:
        path = self.dbase.get_tsf_file_path_by_id(id_val)
        tsf_data = TSFile(path)
        return tsf_data.src_signal

    def get_seis_z_signal_by_id(
            self, id_val) -> List[Tuple[datetime, int]]:
        extract_signal = []
        path = self.dbase.get_seis_file_path_by_id(id_val)
        bin_data = BinaryFile(path, 0, True)
        z_signal = bin_data.read_signal('Z')
        freq_lim = self.config.get_bandpass_freqs()
        z_signal = band_pass_filter(
            z_signal, bin_data.resample_frequency, *freq_lim)
        for i in range(z_signal.shape[0]):
            datetime_val = bin_data.read_date_time_start + timedelta(
                seconds=i / bin_data.resample_frequency)
            extract_signal.append((datetime_val, z_signal[i]))
        return extract_signal

    def create_plot(self, chain_id: int, time_intersection_id: int):
        grav_dt_start = \
            self.dbase.get_start_datetime_gravity_measures_by_time_intersection_id(
                time_intersection_id)
        intersection_dt_start = \
            self.dbase.get_start_datetime_intersection_info_by_id(time_intersection_id)

        skip_minutes_count = int(
            (intersection_dt_start - grav_dt_start).total_seconds() / 60)

        gravity_measures = self.dbase.get_gravity_measures_by_file_id(1028)
        grav_level = self.dbase.get_gravity_level_by_time_intersection_id(97)
        seis_energy = self.dbase.get_seis_energy_by_time_intersection_id(97)
        corrections = \
            self.dbase.get_seis_corrections_by_time_intersection_id(97)

        corrected_grav_measures = gravity_measures.copy()
        for i in range(skip_minutes_count, len(gravity_measures), 1):
            corrected_grav_measures[i] += corrections[i - skip_minutes_count]
            corrected_grav_measures[i] = round(corrected_grav_measures[i], 4)

        z_energy = [x[2] for x in seis_energy]

    def run(self):
        grav_dt_start = \
            self.dbase.get_start_datetime_gravity_measures_by_time_intersection_id(97)
        intersection_dt_start = \
            self.dbase.get_start_datetime_intersection_info_by_id(97)

        skip_minutes_count = int(
            (intersection_dt_start - grav_dt_start).total_seconds() / 60)

        gravity_measures = self.dbase.get_gravity_measures_by_file_id(1028)
        grav_level = self.dbase.get_gravity_level_by_time_intersection_id(97)
        seis_energy = self.dbase.get_seis_energy_by_time_intersection_id(97)
        corrections = \
            self.dbase.get_seis_corrections_by_time_intersection_id(97)

        corrected_grav_measures = gravity_measures.copy()
        for i in range(skip_minutes_count, len(gravity_measures), 1):
            corrected_grav_measures[i] += corrections[i - skip_minutes_count]
            corrected_grav_measures[i] = round(corrected_grav_measures[i], 4)

        z_energy = [x[2] for x in seis_energy]
        create_plot(gravity_measures, corrected_grav_measures, z_energy,
                    grav_level, 'efefrerferf', '/media/michael/Data/TEMP')

    def run2(self):
        time_intersection = 97
        tsf_file_id = 1040
        seis_file_id = 1

        tsf_signal = self.get_tsf_file_signal_by_id(tsf_file_id)
        z_signal = self.get_seis_z_signal_by_id(seis_file_id)

        t_min = max(tsf_signal[0][0], z_signal[0][0])
        t_max = min(tsf_signal[-1][0], z_signal[-1][0])

        tsf_signal = [x for x in tsf_signal if t_min <= x[0] <= t_max]
        z_signal = [x for x in z_signal if t_min <= x[0] <= t_max]

        create_signals_plot(tsf_signal, z_signal)
        pass






if __name__ == '__main__':
    conf_file = '/media/michael/Data/Projects/GraviSeismicComparation' \
                '/ZapolarnoeDeposit/2021/config.json'
    logging.basicConfig(level=logging.DEBUG)
    p = Plotting(conf_file)
    p.run()
    p.run2()
