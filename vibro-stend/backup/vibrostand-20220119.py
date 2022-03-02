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
       '2022_МОСКВА_МФТИ_ИДГ/2022_01_19/19-01-2022_bin/' \
       'K07_2022-01-19_10-06-23'
filename = 'K07_2022-01-19_10-06-23.xx'

stend_modes = [
    StendMeasure(0.1, 1.400, 1358, 2409),
    StendMeasure(0.2, 1.261, 2755, 3405),
    StendMeasure(0.3, 1.168, 3514, 4218),
    StendMeasure(0.4, 1.154, 4298, 5033),
    StendMeasure(0.5, 1.168, 5301, 5868),
    StendMeasure(0.6, 1.210, 6166, 6826),
    StendMeasure(0.7, 1.196, 7076, 7676),
    StendMeasure(0.8, 1.154, 7900, 8431),
    StendMeasure(0.9, 1.116, 8599, 9163),
    StendMeasure(1.0, 1.153, 9384, 9970),
    StendMeasure(1.1, 1.112, 10325, 11012),
    StendMeasure(1.2, 1.145, 11200, 11910),
    StendMeasure(1.3, 1.183, 12138, 12786),
    StendMeasure(1.4, 1.144, 13066, 13792),
    StendMeasure(1.5, 0.712, 13939, 15173),
    StendMeasure(1.6, 0.536, 15544, 16103)
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
