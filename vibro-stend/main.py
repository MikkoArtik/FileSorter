from config import StreamConfig
from processing import MainProcessing


if __name__ == '__main__':
    path = '/media/michael/Data/Projects/GraviSeismicComparation/' \
           'Vibrostend/statistics.yml'
    main_config = StreamConfig(path)

    main_proc = MainProcessing(main_config)
    main_proc.run()
    header = ['DateTime', 'Seismometer', 'Gravimeter', 'Freq', 'Amplitude',
              'Velocity', 'Spectrum-Energy(Seis)', 'Amplitude-Energy(Seis)',
              'Delta-Amplitude(Seis)', 'Gravity', 'GravCorr', 'Acceleration']
    with open('statistics0.dat', 'w') as file_ctx:
        file_ctx.write('\t'.join(header) + '\n')
        for pair in main_proc.pairs:
            records = pair.get_join_data()
            seis_data = pair.seis_data
            for record in records:
                file_ctx.write(record.line + '\n')
