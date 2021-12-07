import sys
import os

from typing import Dict, Tuple, List, NamedTuple

import numpy as np

import pyqtgraph as pg

import PyQt5
from PyQt5 import QtCore
from PyQt5.QtWidgets import *
from PyQt5.uic import *

from seiscore import BinaryFile
from seiscore import Spectrogram

from config import ConfigFile
from dbase import SqliteDbase


def get_lib_path() -> list:
    return [os.path.join(os.path.dirname(PyQt5.__file__), 'Qt5', 'plugins')]


class FormData(NamedTuple):
    filename: str
    component: str
    resample_freq: int
    min_frequency: float
    max_frequency: float
    conclusion: str


class MainWindow:
    def __init__(self, database: SqliteDbase):
        self.__app = QApplication(sys.argv)
        self.__window = QMainWindow()
        self.__dbase = database

        ui_path = 'SeisDefectViewer.ui'
        self.__ui = loadUi(ui_path, self.__window)
        self.__ui.cbFilesList.currentTextChanged.connect(self.show_signal_data)
        self.__ui.cbComponentList.currentTextChanged.connect(self.show_signal_data)
        self.__ui.bSave.clicked.connect(self.save_checking_conclusion)
        self.__ui.sbFMin.valueChanged.connect(self.set_spectrogram_y_limits)
        self.__ui.sbFMax.valueChanged.connect(self.set_spectrogram_y_limits)

        self.__files_info = None
        self.__spectrogram_plot = None
        self.update_lists()

        self.show_signal_data()

        self.screen_center()
        self.__window.show()
        self.__app.exec()

    @property
    def window(self):
        return self.__window

    @property
    def ui(self):
        return self.__ui

    @property
    def dbase(self) -> SqliteDbase:
        return self.__dbase

    @property
    def form_data(self) -> FormData:
        filename = self.ui.cbFilesList.currentText()
        component = self.ui.cbComponentList.currentText()
        resample_freq = self.ui.sbResampleFreq.value()
        f_min = self.ui.sbFMin.value()
        f_max = self.ui.sbFMax.value()
        conclusion = self.ui.cbConclusion.currentText()
        return FormData(filename, component, resample_freq, f_min, f_max,
                        conclusion)

    def get_files_list(self) -> Dict[str, Tuple[int, str, List[str]]]:
        records = self.dbase.get_seismic_files_for_checking()
        transform_data = dict()
        for rec in records:
            path = rec[1]
            filename = os.path.basename(path)
            transform_data[filename] = rec
        return transform_data

    def screen_center(self):
        frame_geom = self.window.frameGeometry()
        screen = QApplication.desktop().screenNumber(QApplication.desktop().cursor().pos())
        center_point = QApplication.desktop().screenGeometry(screen).center()
        frame_geom.moveCenter(center_point)
        self.window.move(frame_geom.topLeft())

    def set_files_list(self):
        self.ui.cbFilesList.clear()
        self.ui.cbFilesList.addItems(list(self.__files_info.keys()))

    def get_current_file_info(self) -> Tuple[int, str, List[str]]:
        return self.__files_info[self.form_data.filename]

    def set_components_list(self):
        self.ui.cbComponentList.clear()

        file_info = self.get_current_file_info()
        components = file_info[-1]
        self.ui.cbComponentList.addItems(components)

    def set_spectrogram_y_limits(self):
        if not self.__spectrogram_plot:
            return
        f_min = self.form_data.min_frequency
        f_max = self.form_data.max_frequency
        self.__spectrogram_plot.setRange(yRange=(f_min, f_max))

    def get_current_component(self) -> str:
        return self.form_data.component

    def get_signal(self) -> Tuple[np.ndarray, int]:
        _, path, _ = self.get_current_file_info()
        component = self.get_current_component()
        resample_freq = self.form_data.resample_freq

        bin_data = BinaryFile(path, use_avg_values=True,
                              resample_frequency=resample_freq)
        return bin_data.read_signal(component), bin_data.resample_frequency

    def plot_signal(self, signal: np.ndarray, frequency: int):
        widget = self.ui.gSignal
        widget.clear()
        time_scale = np.arange(0, signal.shape[0], 1) / frequency
        widget.plot(time_scale, signal, pen=(255, 0, 0))

    def plot_spectrogram(self, signal: np.ndarray, frequency: int):
        plot = self.ui.gSpectrogram
        plot.clear()

        spectrogram = Spectrogram(signal, frequency)
        sp_data = spectrogram.sp_data

        if sp_data.frequencies.shape[0] == 0:
            return

        time, frequencies = sp_data.times, sp_data.frequencies
        amplitudes = sp_data.amplitudes

        amplitudes = 20 * np.log10(abs(amplitudes))
        amplitudes = amplitudes.T

        img = pg.ImageItem()
        img.setImage(amplitudes, xvals=time, yvals=frequencies)

        dx = (time[-1] - time[0]) / time.shape[0]
        dy = (frequencies[-1] - frequencies[0]) / frequencies.shape[0]
        img.scale(dx, dy)

        hist = pg.HistogramLUTItem()
        min_val, max_val = spectrogram.scale_limits()
        hist.setLevels(min_val, max_val)
        hist.gradient.restoreState(
            {'mode': 'rgb',
             'ticks': [
                 (0.0, (153, 102, 255, 255)),
                 (0.2, (0, 0, 255, 255)),
                 (0.4, (0, 255, 0, 255)),
                 (0.6, (255, 255, 0, 255)),
                 (0.8, (255, 102, 0, 255)),
                 (1.0, (255, 0, 0, 255))]
             })
        hist.setImageItem(img)
        plot.addItem(img)
        self.__spectrogram_plot = plot
        self.set_spectrogram_y_limits()

    def show_signal_data(self):
        if not self.get_current_component():
            return
        try:
            signal, frequency = self.get_signal()
        except KeyError:
            return
        self.plot_signal(signal, frequency)
        self.plot_spectrogram(signal, frequency)

    def save_checking_conclusion(self):
        filename = self.ui.cbFilesList.currentText()
        component = self.ui.cbComponentList.currentText()
        conclusion = self.ui.cbConclusion.currentText()
        file_id = self.__files_info[filename][0]
        self.dbase.update_seis_file_checking_status(file_id, component, conclusion)
        self.update_lists()

    def update_lists(self):
        self.__files_info = self.get_files_list()
        if not self.__files_info:
            self.ui.statusBar.showMessage('Все файлы уже отбракованы')
            return

        self.set_files_list()
        self.set_components_list()


def run():
    QtCore.QCoreApplication.setLibraryPaths(get_lib_path())
    MainWindow(db)


if __name__ == '__main__':
    # storage_folder = os.getenv('DATA_PATH')
    conf_file = '/media/michael/Data/Projects/GraviSeismicComparation' \
                '/ZapolarnoeDeposit/2021/config.json'
    config = ConfigFile(conf_file)
    db = SqliteDbase(config.export_root)
    run()
