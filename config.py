from datetime import datetime
import os
import json
import logging
from typing import List, NamedTuple


STRUCTURE = {
    'log': {
        'path': 'path',
        'columns': {
            'date': 0,
            'point': 1,
            'seis_list': 2,
            'grav_list': 3,
            'is_bad': 5
        },
        'list_delimiter': ',',
        'is_good_keyword': 'False'
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
                'order': 0,
                'point': 1,
                'sensor': 2,
                'date': 3,
                'time': 4
            }
        }
    },
    'export': {
        'root': 'path'
    }
}


class SeismicFileAttr(NamedTuple):
    name: str
    order: int
    point: str
    datetime: datetime
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
        order_index, point_index = markers['order'], markers['point']
        date_index, time_index = markers['date'], markers['time']
        sensor_index = markers['sensor']

        split_name = filename.split('.')[0].split(delimiter)
        order = int(split_name[order_index])
        point = split_name[point_index]

        date_split = split_name[date_index].split('-')
        year, month, day = map(int, date_split)

        time_split = split_name[time_index].split('-')
        hour, minute, second = map(int, time_split)
        datetime_val = datetime(year, month, day, hour, minute, second)
        sensor = split_name[sensor_index]

        return SeismicFileAttr(filename, order, point, datetime_val, sensor)
