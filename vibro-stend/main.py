from config import StreamConfig
from processing import Processing


if __name__ == '__main__':
    path = '/media/michael/Data/Projects/GraviSeismicComparation/' \
           '2022_МОСКВА_МФТИ_ИДГ/statistics.yml'
    main_config = StreamConfig(path)

    header = ['Date', 'Seismometer', 'Gravimeter', 'Freq', 'Amplitude',
              'Velocity', 'Spectrum-Energy(Seis)', 'Amplitude-Energy(Seis)',
              'Delta-Amplitude(Seis)', 'Spectrum-Energy(Grav)',
              'Amplitude-Energy(Grav)', 'Delta-Amplitude(Grav)']
    with open('statistics.dat', 'w') as file_ctx:
        file_ctx.write('\t'.join(header) + '\n')
        for file_path in main_config.config_files:
            p = Processing(file_path)
            p.run()

            pair = p.config.device_pair
            stat_data = p.get_cycle_statistics()

            measure_date = p.config.measure_date.strftime('%Y-%m-%d')
            for stat_line in stat_data:
                tmp = [measure_date, str(pair.seismic),
                       str(pair.gravimetric)]
                tmp += [str(x) for x in stat_line[:3]]
                tmp += [str(int(x)) for x in stat_line[3:6]]
                tmp += [str(round(x, 3)) for x in stat_line[6:]]
                line = '\t'.join(tmp)
                file_ctx.write(line + '\n')
