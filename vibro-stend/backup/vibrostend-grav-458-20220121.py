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


root = '/media/michael/Data/Projects/GraviSeismicComparation/2022_МОСКВА_МФТИ_ИДГ/Gravimetric/21-01-2022/458_2022_01_21'
filename = '458_2022_01_21_2.smp'
signal_frequency = 6
g_cal1 = 8329.075

stend_modes = [
    StendMeasure(0.9, 0.156, 712, 1137),
    StendMeasure(0.9, 0.202, 1499, 1847),
    StendMeasure(0.9, 0.325, 2174, 2615),
    StendMeasure(0.9, 0.427, 2950, 3443),
    StendMeasure(0.9, 0.520, 3954, 4354),
    StendMeasure(0.9, 0.621, 4632, 5103),
    StendMeasure(0.9, 0.780, 5452, 5938),
    StendMeasure(0.9, 0.940, 6449, 6852),
    StendMeasure(0.9, 1.043, 7445, 7804),
    StendMeasure(0.9, 1.174, 8292, 8662),
    StendMeasure(0.9, 1.190, 9011, 9385),
    StendMeasure(0.9, 1.447, 9715, 10246),
    StendMeasure(0.9, 1.595, 10554, 10965),
    StendMeasure(0.9, 1.899, 11382, 11782),
    StendMeasure(0.9, 2.085, 12075, 12479),
    StendMeasure(0.9, 2.570, 12881, 13224),
    StendMeasure(0.9, 2.827, 13622, 14030),
    StendMeasure(0.9, 3.017, 14462, 14955),
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
