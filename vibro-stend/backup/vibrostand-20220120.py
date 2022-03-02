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
       '2022_МОСКВА_МФТИ_ИДГ/2022_01_20/20-01-2022_bin/' \
       'K07_2022-01-20_08-21-05'
filename = 'K07_2022-01-20_08-21-05.xx'

stend_modes = [
    StendMeasure(0.1, 1.085, 1029, 1516),
    StendMeasure(0.2, 1.185, 1851, 2301),
    StendMeasure(0.3, 1.245, 2651, 3081),
    StendMeasure(0.4, 1.167, 3439, 3930),
    StendMeasure(0.5, 1.083, 4290, 4774),
    StendMeasure(0.6, 1.039, 5167, 5583),
    StendMeasure(0.7, 1.081, 5886, 6312),
    StendMeasure(0.8, 1.168, 6689, 7100),
    StendMeasure(0.9, 1.160, 7438, 7895),
    StendMeasure(1.0, 1.247, 8234, 8690),
    StendMeasure(1.1, 1.362, 8999, 9441),
    StendMeasure(1.2, 1.340, 9783, 10192),
    StendMeasure(1.3, 1.297, 10537, 10962),
    StendMeasure(1.4, 1.236, 11271, 11743),
    StendMeasure(1.5, 1.225, 12044, 12545),
    StendMeasure(1.6, 1.141, 12888, 13363),
    StendMeasure(0.1, 0.419, 15223, 15629),
    StendMeasure(0.2, 0.456, 15995, 16431),
    StendMeasure(0.3, 0.429, 16760, 17172),
    StendMeasure(0.4, 0.435, 17546, 17976),
    StendMeasure(0.5, 0.413, 18336, 18752),
    StendMeasure(0.6, 0.405, 19100, 19577),
    StendMeasure(0.7, 0.410, 20047, 20415),
    StendMeasure(0.8, 0.393, 20910, 21311),
    StendMeasure(0.9, 0.414, 21802, 22195),
    StendMeasure(1.0, 0.453, 22602, 22976),
    StendMeasure(1.1, 0.398, 23336, 23942),
    StendMeasure(1.2, 0.464, 24346, 24784)
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