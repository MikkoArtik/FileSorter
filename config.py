import os
import json
import logging
from typing import List, NamedTuple, Tuple


STRUCTURE = {
    'geometry': {
        'filename': 'point_coords.csv',
        'columns': {
            'point_name': 0,
            'xWGS84': 2,
            'yWGS84': 3
        }
    },
    'gravimetric': {
        'root': 'path'
    },
    'seismic': {
        'root': 'path',
        'filename': {
            'extensions': ['bin', 'xx', '00'],
            'delimiter': '_',
            'markers': {
                'point': 1,
                'sensor': 2,
            }
        }
    },
    'processing': {
        'f_min': 0.1,
        'f_max': 10,
        'epsilon': 3
    },
    'export': {
        'root': 'path'
    }
}


class SeismicFileAttr(NamedTuple):
    name: str
    point: str
    sensor: str


def create_config_file(folder_path: str):
    path = os.path.join(folder_path, 'config_template.json')
    with open(path, 'w') as file_ctx:
        json.dump(STRUCTURE, file_ctx, indent=4)


class ConfigFile:
    def __init__(self, path: str):
        if not os.path.exists(path):
            logging.error(f'Configuration file {path} not found')
            raise OSError

        self.path = path
        self.data = self.__load_file()

    def __load_file(self) -> dict:
        with open(self.path) as file_ctx:
            return json.load(file_ctx)

    @property
    def export_root(self) -> str:
        return self.data['export']['root']

    @property
    def seismic_root(self) -> str:
        return self.data['seismic']['root']

    @property
    def gravimetric_root(self) -> str:
        return self.data['gravimetric']['root']

    @property
    def seismic_extensions(self) -> List[str]:
        return self.data['seismic']['filename']['extensions']

    def is_seismic_file(self, filename: str) -> bool:
        extension = filename.split('.')[-1]
        return any([x == extension for x in self.seismic_extensions])

    def get_seismic_file_attr(self, filename: str) -> SeismicFileAttr:
        delimiter = self.data['seismic']['filename']['delimiter']

        markers = self.data['seismic']['filename']['markers']
        point_index = markers['point']
        sensor_index = markers['sensor']

        split_name = filename.split('.')[0].split(delimiter)
        point = split_name[point_index]

        sensor = split_name[sensor_index]
        return SeismicFileAttr(filename, point, sensor)

    def get_bandpass_freqs(self) -> Tuple[float, float]:
        params = self.data['processing']
        return params['f_min'], params['f_max']
