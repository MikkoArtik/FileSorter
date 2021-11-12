import os
import sqlite3
from datetime import datetime


DEFAULT_NAME = 'Project.db'
DBASE_SCRIPT = 'dbase.sql'


def load_dbase_script(path) -> str:
    with open(path) as file_ctx:
        return file_ctx.read()


class SqliteDbase:
    def __init__(self, root=''):
        self.root = root
        self.connection = self.create_connection()

    @property
    def path(self) -> str:
        return os.path.join(self.root, DEFAULT_NAME)

    def create_connection(self):
        if os.path.exists(self.path):
            return sqlite3.connect(self.path)

        connection = sqlite3.connect(self.path)
        script_text = load_dbase_script(DBASE_SCRIPT)
        cursor = connection.cursor()
        cursor.executescript(script_text)
        connection.commit()
        cursor.close()
        return connection

    def add_gravimeter(self, number: str) -> int:
        query = f'INSERT INTO gravimeters(number) VALUES(\'{number}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

        query = f'SELECT id FROM gravimeters WHERE number=\'{number}\';'
        id_val = cursor.execute(query).fetchone()[0]
        return id_val

    def add_seismometer(self, number: str):
        query = f'INSERT INTO seismometers(number) VALUES(\'{number}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

        query = f'SELECT id FROM seismometers WHERE number=\'{number}\';'
        id_val = cursor.execute(query).fetchone()[0]
        return id_val

    def add_station(self, name: str) -> int:
        query = f'INSERT INTO stations(name) VALUES(\'{name}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

        query = f'SELECT id FROM stations WHERE name=\'{name}\';'
        id_val = cursor.execute(query).fetchone()[0]
        return id_val

    def add_dat_file(self, grav_number: str, station: str,
                     datetime_start: datetime, datetime_stop: datetime,
                     path: str):
        sensor_id = self.add_gravimeter(grav_number)
        point_id = self.add_station(station)
        query = 'INSERT INTO dat_files(gravimeter_id, station_id, ' \
                'datetime_start, datetime_stop, path) VALUES ' \
                f'({sensor_id}, {point_id}, \'{datetime_start}\', ' \
                f'\'{datetime_stop}\', \'{path}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

    def add_tsf_file(self, dev_num_part: str, datetime_start: datetime,
                     datetime_stop: datetime, path: str):
        query = 'INSERT INTO tsf_files(dev_num_part, datetime_start, ' \
                                      'datetime_stop, path) ' \
                f'VALUES (\'{dev_num_part}\', \'{datetime_start}\', ' \
                f'\'{datetime_stop}\', \'{path}\')'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

    def add_seis_file(self, sensor: str, station: str,
                      datetime_start: datetime, datetime_stop: datetime,
                      path: str):
        sensor_id = self.add_seismometer(sensor)
        station_id = self.add_station(station)

        query = 'INSERT INTO seis_files(sensor_id, station_id, ' \
                'datetime_start, datetime_stop, path) ' \
                f'VALUES ({sensor_id}, {station_id}, ' \
                f'\'{datetime_start}\', \'{datetime_stop}\', \'{path}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

