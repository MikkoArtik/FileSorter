from config import StreamConfig
from processing import Processing


if __name__ == '__main__':
    path = '/media/michael/Data/Projects/GraviSeismicComparation/' \
           'Vibrostend/statistics.yml'
    main_config = StreamConfig(path)

    header = ['DateTime', 'Seismometer', 'Gravimeter', 'Freq', 'Amplitude',
              'Velocity', 'Spectrum-Energy(Seis)', 'Amplitude-Energy(Seis)',
              'Delta-Amplitude(Seis)', 'Gravity']
    with open('statistics0.dat', 'w') as file_ctx:
        file_ctx.write('\t'.join(header) + '\n')
        for file_path in main_config.config_files:
            p = Processing(file_path)
            for record in p.get_statistics():
                file_ctx.write(record.line + '\n')
