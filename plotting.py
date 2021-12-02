import logging
import os
from typing import List

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from config import ConfigFile
from dbase import SqliteDbase


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
    plt.savefig(export_path)
    plt.close()


class Plotting:
    def __init__(self, config_file_path: str):
        if not os.path.exists(config_file_path):
            raise OSError

        self.config = ConfigFile(config_file_path)
        self.dbase = SqliteDbase(self.config.export_root)
        self.logger = logging.getLogger('Plotting')

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
        pass


if __name__ == '__main__':
    conf_file = '/media/michael/Data/Projects/GraviSeismicComparation' \
                '/ZapolarnoeDeposit/2021/config.json'
    logging.basicConfig(level=logging.DEBUG)
    p = Plotting(conf_file)
    p.run()
