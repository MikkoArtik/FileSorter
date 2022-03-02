import os
from dataclasses import dataclass

import numpy as np

from seiscore import BinaryFile
from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy


@dataclass
class StendMeasure:
    frequency: float
    amplitude: float
    t_min: int
    t_max: int


root = '/media/michael/Data/Projects/GraviSeismicComparation/' \
       '2022_МОСКВА_МФТИ_ИДГ/2022_01_21/21-01-2022_bin/' \
       'K07_2022-01-21_07-58-10'
filename = 'K07_2022-01-21_07-58-10.xx'

stend_modes = [
    StendMeasure(0.9, 0.156, 843, 1263),
    StendMeasure(0.9, 0.202, 1651, 2024),
    StendMeasure(0.9, 0.325, 2458, 2828),
    StendMeasure(0.9, 0.427, 3369, 3737),
    StendMeasure(0.9, 0.520, 4422, 4730),
    StendMeasure(0.9, 0.621, 5278, 5551),
    StendMeasure(0.9, 0.780, 6166, 6496),
    StendMeasure(0.9, 0.940, 7097, 7500),
    StendMeasure(0.9, 1.043, 8156, 8500),
    StendMeasure(0.9, 1.174, 9656, 10015),
    StendMeasure(0.9, 1.190, 10468, 10831),
    StendMeasure(0.9, 1.447, 11394, 11789),
    StendMeasure(0.9, 1.595, 12137, 12554),
    StendMeasure(0.9, 1.899, 13006, 13435),
    StendMeasure(0.9, 2.085, 13775, 14224),
    StendMeasure(0.9, 2.570, 14568, 15066),
    StendMeasure(0.9, 2.827, 15468, 15904),
    StendMeasure(0.9, 3.017, 16249, 16893),
]


bin_path = os.path.join(root, filename)
time_offset_sec = 0
time_interval = 10
resample_frequency = 10


bin_data = BinaryFile(bin_path)
z_signal = bin_data.read_signal('Z')
signal_time = np.arange(0, z_signal.shape[0]) / bin_data.signal_frequency
z_signal = np.column_stack((signal_time, z_signal))

parts_count = int(z_signal.shape[0] / (time_interval *
                                       bin_data.signal_frequency))

result = np.zeros(shape=(parts_count, 3))
result[:, 0] = time_interval / 2 + np.arange(0, parts_count) * time_interval
for i in range(parts_count):
    left_edge = int(i * time_interval * bin_data.signal_frequency)
    right_edge = int((i + 1) * time_interval * bin_data.signal_frequency)

    signal = z_signal[left_edge: right_edge, 1]
    min_amp, max_amp = min(signal), max(signal)
    signal -= int(np.mean(signal))

    spectrum_data = spectrum(signal, bin_data.resample_frequency)
    e_val = spectrum_energy(spectrum_data, [0, 2])
    result[i, 1] = e_val

    ampl_diff = max_amp - min_amp
    result[i, 2] = ampl_diff

export_path = os.path.join(root, 'energy-amplitude.dat')
np.savetxt(export_path, result, '%i\t%f\t%i',
           header='Time,sec\tEnergy\tAmplitude', comments='')

export_path = os.path.join(root, 'source_signal.dat')
np.savetxt(export_path, z_signal, '%.3f\t%i', '\t',
           header='Time\tAmplitude', comments='')

for index, mode in enumerate(stend_modes):
    signal = z_signal[(z_signal[:, 0] >= mode.t_min) *
                      (z_signal[:, 0] < mode.t_max), 1]
    spectrum_data = spectrum(signal, bin_data.resample_frequency)

    filename = f'{index}-stand-frequency={mode.frequency}-' \
               f'amplitude={mode.amplitude}.dat'
    path = os.path.join(root, filename)
    header = 'Frequency\tAmplitude'
    np.savetxt(path, spectrum_data, '%f', '\t', header=header, comments='')

bin_data.resample_frequency = resample_frequency
resample_signal = bin_data.read_signal('Z')
signal_time = np.arange(0, resample_signal.shape[0]) / bin_data.resample_frequency
resample_z_signal = np.column_stack((signal_time, resample_signal))

export_path = os.path.join(root, 'source_signal_resample.dat')
np.savetxt(export_path, resample_z_signal, '%.3f\t%i', '\t',
           header='Time\tAmplitude', comments='')
