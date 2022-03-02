import os
from dataclasses import dataclass

import numpy as np

from seiscore.functions.spectrum import spectrum
from seiscore.functions.energy import spectrum_energy


@dataclass
class StendMeasure:
    frequency: float
    amplitude: float
    t_min: int
    t_max: int


root = '/media/michael/Data/Projects/GraviSeismicComparation/2022_МОСКВА_МФТИ_ИДГ/Gravimetric/20-01-2022/1418_2022_01_20'
filename = '1418_2022_01_20.smp'
signal_frequency = 6
g_cal1 = 8238.908

stend_modes = [
    StendMeasure(0.1, 0.419, 999, 1451),
    StendMeasure(0.2, 0.456, 1692, 2169),
    StendMeasure(0.3, 0.429, 2429, 2851),
    StendMeasure(0.4, 0.435, 3119, 3583),
    StendMeasure(0.5, 0.413, 3829, 4293),
    StendMeasure(0.6, 0.405, 4548, 5060),
    StendMeasure(0.7, 0.41, 5339, 5848),
    StendMeasure(0.8, 0.393, 6136, 6597),
    StendMeasure(0.9, 0.414, 6930, 7418),
    StendMeasure(1, 0.453, 7742, 8152),
    StendMeasure(1.1, 0.398, 8482, 9059),
    StendMeasure(1.2, 0.464, 9366, 9829),
]

grav_smp_file_path = os.path.join(root, filename)
time_offset_sec = 0
time_interval = 10


grav_data = np.loadtxt(grav_smp_file_path, skiprows=1)
signal_time = np.arange(0, grav_data.shape[0]) / signal_frequency
grav_signal = np.column_stack((signal_time, grav_data[:, 0]))
grav_signal[:, 1] *= g_cal1 / 536870912

parts_count = int(grav_signal.shape[0] / (time_interval * signal_frequency))

result = np.zeros(shape=(parts_count, 3))
result[:, 0] = time_interval / 2 + np.arange(0, parts_count) * time_interval
for i in range(parts_count):
    left_edge = int(i * time_interval * signal_frequency)
    right_edge = int((i + 1) * time_interval * signal_frequency)

    signal = grav_signal[left_edge: right_edge, 1].copy()
    min_amp, max_amp = min(signal), max(signal)
    signal -= np.mean(signal)

    spectrum_data = spectrum(signal, signal_frequency)
    e_val = spectrum_energy(spectrum_data, [0, 2])
    result[i, 1] = e_val

    ampl_diff = max_amp - min_amp
    result[i, 2] = ampl_diff

export_path = os.path.join(root, 'energy-amplitude.dat')
np.savetxt(export_path, result, '%i\t%f\t%.3f',
           header='Time,sec\tEnergy\tAmplitude', comments='')

export_path = os.path.join(root, 'source_signal.dat')
np.savetxt(export_path, grav_signal, '%.3f\t%.3f', '\t',
           header='Time\tAmplitude', comments='')

for index, mode in enumerate(stend_modes):
    signal = grav_signal[(grav_signal[:, 0] >= mode.t_min) *
                         (grav_signal[:, 0] < mode.t_max), 1]
    spectrum_data = spectrum(signal, signal_frequency)

    filename = f'{index}-stand-frequency={mode.frequency}-' \
               f'amplitude={mode.amplitude}.dat'
    path = os.path.join(root, filename)
    header = 'Frequency\tAmplitude'
    np.savetxt(path, spectrum_data, '%f\t%.3f', '\t', header=header,
               comments='')
