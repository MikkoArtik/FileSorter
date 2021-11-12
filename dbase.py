import os
import sqlite3
from datetime import datetime
from typing import Union
import logging


DEFAULT_NAME = 'Project.db'
DBASE_SCRIPT = 'dbase.sql'


def load_dbase_script(path) -> str:
    with open(path) as file_ctx:
        return file_ctx.read()


class SqliteDbase:
    def __init__(self, root=''):
        self.root = root
        self.logger = logging.getLogger('dbase')
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

    def add_chain(self, sensor_part_name: str, path: str) -> int:
        query = 'INSERT INTO chains(dev_num_part, path) ' \
                f'VALUES (\'{sensor_part_name}\', \'{path}\')'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new chain with path {path} successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new chain with path {path} failed')

        query = f'SELECT id FROM chains WHERE path=\'{path}\''
        id_val = cursor.execute(query).fetchone()[0]
        self.logger.info(f'chain: path={path} '
                         f'sensor_part_name={sensor_part_name} id={id_val}')
        return id_val

    def add_link(self, chain_id: int, order: int, filename: str) -> int:
        query = 'INSERT INTO links(chain_id, link_id, filename) VALUES ' \
                f'({chain_id}, {order}, \'{filename}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new link {filename} successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new link {filename} failed')

        query = f'SELECT id FROM links WHERE filename=\'{filename}\''
        id_val = cursor.execute(query).fetchone()[0]
        self.logger.info(f'link: filename={filename} order={order} '
                         f'chain_id={chain_id} id={id_val}')
        return id_val

    def add_gravimeter(self, number: str) -> int:
        query = f'INSERT INTO gravimeters(number) VALUES(\'{number}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new gravimeter with number {number} '
                              'successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new gravimeter with number {number} '
                              'failed')

        query = f'SELECT id FROM gravimeters WHERE number=\'{number}\';'
        id_val = cursor.execute(query).fetchone()[0]
        self.logger.info(f'gravimeter: number={number} id={id_val}')
        return id_val

    def add_seismometer(self, number: str):
        query = f'INSERT INTO seismometers(number) VALUES(\'{number}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new seismometer with number {number} '
                              'successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new seismometer with number {number} '
                              'failed')

        query = f'SELECT id FROM seismometers WHERE number=\'{number}\';'
        id_val = cursor.execute(query).fetchone()[0]
        self.logger.info(f'seismometer: number={number} id={id_val}')
        return id_val

    def add_station(self, name: str) -> int:
        query = f'INSERT INTO stations(name) VALUES(\'{name}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new station with name {name} '
                              'successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new station with name {name} '
                              'failed')

        query = f'SELECT id FROM stations WHERE name=\'{name}\';'
        id_val = cursor.execute(query).fetchone()[0]
        self.logger.info(f'station: name={name} id={id_val}')
        return id_val

    def get_link_id(self, filename: str) -> Union[None, int]:
        query = f'SELECT id FROM links WHERE filename=\'{filename}\';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchone()
        if not records:
            self.logger.debug(f'link id for filename {filename} not found')
            return None
        id_val = records[0]
        self.logger.info(f'link: filename={filename} id={id_val}')
        return id_val

    def change_link_status(self, link_id: int, is_exist=True):
        query = f'UPDATE links SET is_exist={int(is_exist)} ' \
                f'WHERE id={link_id};'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'status for link with id {link_id} changed '
                              f'to {is_exist}')
        except sqlite3.IntegrityError:
            self.logger.error(f'status for link with id {link_id} not change')

    def add_dat_file(self, grav_number: str, station: str,
                     datetime_start: datetime, datetime_stop: datetime,
                     path: str):
        filename = os.path.basename(path)
        link_id = self.get_link_id(filename)
        if not link_id:
            return
        else:
            self.change_link_status(link_id, True)

        sensor_id = self.add_gravimeter(grav_number)
        point_id = self.add_station(station)

        query = 'INSERT INTO dat_files(gravimeter_id, station_id, ' \
                'link_id, datetime_start, datetime_stop, path) VALUES ' \
                f'({sensor_id}, {point_id}, {link_id}, ' \
                f'\'{datetime_start}\', \'{datetime_stop}\', \'{path}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'DAT-file with path {path} added successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'DAT-file with path {path} not add to dbase')

    def add_tsf_file(self, dev_num_part: str, datetime_start: datetime,
                     datetime_stop: datetime, path: str):
        query = 'INSERT INTO tsf_files(dev_num_part, datetime_start, ' \
                                      'datetime_stop, path) ' \
                f'VALUES (\'{dev_num_part}\', \'{datetime_start}\', ' \
                f'\'{datetime_stop}\', \'{path}\')'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'TSF-file with path {path} added successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'TSF-file with path {path} not add')

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
            self.logger.debug(f'seismic file with path {path} added '
                              f'successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'seismic file with path {path} not add')
