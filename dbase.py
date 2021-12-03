import os
import sqlite3
from datetime import datetime
from typing import Union, List, Tuple, Set
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

    def add_station(self, name: str, x_wgs84=0., y_wgs84=0.) -> int:
        cursor = self.connection.cursor()
        query = f'SELECT COUNT(1) FROM stations WHERE name=\'{name}\';'
        cursor.execute(query)
        record = cursor.fetchone()[0]
        if not record:
            query = f'INSERT INTO stations(name, xWGS84, yWGS84) ' \
                    f'VALUES(\'{name}\', {x_wgs84}, {y_wgs84});'
        else:
            query = f'UPDATE stations SET xWGS84={x_wgs84}, ' \
                    f'yWGS84={y_wgs84} WHERE name=\'{name}\';'
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert/update new station with name {name} '
                              'successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert/update new station with name {name} '
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

    def get_id_dat_file_by_path(self, path: str) -> Union[int, None]:
        query = f'SELECT id FROM dat_files WHERE path=\'{path}\''
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchone()
        if not records:
            return None
        else:
            return records[0]

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

    def add_gravity_measure(self, dat_file_id: int, datetime_val: datetime,
                            corr_grav: float):
        query = 'INSERT INTO gravity_measures (dat_file_id, datetime_val, ' \
                                             f'corr_grav) VALUES ' \
                f'({dat_file_id}, \'{datetime_val}\', {corr_grav});'
        self.connection.cursor().execute(query)
        self.connection.commit()

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

        datetime_start_str = datetime_start.strftime('%Y-%m-%d %H:%M:%S')
        datetime_stop_str = datetime_stop.strftime('%Y-%m-%d %H:%M:%S')

        query = 'INSERT INTO seis_files(sensor_id, station_id, ' \
                'datetime_start, datetime_stop, path) ' \
                f'VALUES ({sensor_id}, {station_id}, ' \
                f'\'{datetime_start_str}\', \'{datetime_stop_str}\', ' \
                f'\'{path}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'seismic file with path {path} added '
                              f'successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'seismic file with path {path} not add')

        query = f'SELECT id FROM seis_files WHERE path=\'{path}\''
        cursor = self.connection.cursor()
        cursor.execute(query)
        id_val = cursor.fetchone()[0]

        query = f'INSERT INTO seis_files_defect_info(seis_id) ' \
                f'VALUES ({id_val});'
        self.connection.cursor().execute(query)
        self.connection.commit()

    def get_seismic_files_for_checking(self) -> List[Tuple[int, str, List[str]]]:
        query = 'SELECT * FROM need_check_seis_files;'
        cursor = self.connection.cursor()
        cursor.execute(query)

        records = []
        for rec in cursor.fetchall():
            file_id, path, x_channel, y_channel, z_channel = rec
            components = []
            if x_channel == 'Unknown':
                components.append('X')
            if y_channel == 'Unknown':
                components.append('Y')
            if z_channel == 'Unknown':
                components.append('Z')
            if not components:
                continue
            records.append((file_id, path, components))
        return records

    def update_file_checking(self, file_id: int, component: str,
                             conclusion: str):
        if component.upper() == 'X':
            query = f'UPDATE seis_files_defect_info ' \
                    f'SET x_channel=\'{conclusion}\' WHERE seis_id={file_id};'
        elif component.upper() == 'Y':
            query = f'UPDATE seis_files_defect_info ' \
                    f'SET y_channel=\'{conclusion}\' WHERE seis_id={file_id};'
        elif component.upper() == 'Z':
            query = f'UPDATE seis_files_defect_info ' \
                    f'SET z_channel=\'{conclusion}\' WHERE seis_id={file_id};'
        else:
            return
        self.connection.cursor().execute(query)
        self.connection.commit()

    def get_grav_seis_times(self):
        query = 'SELECT * FROM grav_seis_times;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        src_data = [list(x) for x in cursor.fetchall()]

        records = []
        for rec in src_data:
            record = rec[:2]
            for i in range(4):
                dt = datetime.strptime(rec[2 + i], '%Y-%m-%d %H:%M:%S')
                record.append(dt)
            records.append(record)
        return records

    def clear_time_intersections(self):
        query = 'DELETE FROM time_intersection;'
        self.connection.cursor().execute(query)
        self.connection.commit()

    def add_time_intersection(self, grav_dat_id: int, seis_id,
                              datetime_left: datetime,
                              datetime_right: datetime):
        query = 'INSERT INTO time_intersection(grav_dat_id, seis_id, ' \
                f'datetime_start, datetime_stop) VALUES ({grav_dat_id}, ' \
                f'{seis_id}, \'{datetime_left}\', \'{datetime_right}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'Time intersection added successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'Fail adding time intersection')

    def get_time_intersections(self) -> List[Tuple[int, int, int, datetime,
                                                   datetime]]:
        query = 'SELECT * FROM time_intersection WHERE seis_id IN ' \
                '(SELECT id FROM good_seis_files);'
        cursor = self.connection.cursor()
        cursor.execute(query)
        src_data = [list(x) for x in cursor.fetchall()]

        records = []
        for rec in src_data:
            record = rec[:3]
            for i in range(2):
                dt = datetime.strptime(rec[3 + i], '%Y-%m-%d %H:%M:%S')
                record.append(dt)
            records.append(tuple(record))
        return records

    def get_seis_file_path_by_id(self, id_val: int) -> Union[str, None]:
        query = f'SELECT path from seis_files WHERE id={id_val}'
        cursor = self.connection.cursor()
        cursor.execute(query)
        record = cursor.fetchone()
        if not record:
            self.logger.error(f'Seismic file with id={id_val} not found')
            return None
        else:
            self.logger.info(f'Seismic file: id={id_val} path={record[0]}')
            return record[0]

    def add_minute(self, time_intersection_id: int, minute_index: int) -> int:
        query = 'INSERT INTO minutes_intersection(time_intersection_id, ' \
                                                 'minute_index) VALUES (' \
                f'{time_intersection_id}, {minute_index});'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass

        query = 'SELECT id FROM minutes_intersection ' \
                f'WHERE time_intersection_id={time_intersection_id} AND ' \
                f'minute_index={minute_index};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchone()
        return records[0]

    def delete_all_energies(self):
        query = 'DELETE FROM seis_energy;'
        self.connection.cursor().execute(query)
        self.connection.commit()

    def add_energies(self, time_intersection_id: int,
                     energies: List[List[float]]):
        query_template = 'INSERT INTO seis_energy(minute_id, Ex, Ey, Ez, ' \
                                                 'Efull) ' \
                         'VALUES ({minute_id}, {e_x}, {e_y}, {e_z}, {e_f});'
        for index, energy_xyzf in enumerate(energies):
            minute_id = self.add_minute(time_intersection_id, index)
            query = query_template.format(
                minute_id=minute_id, e_x=energy_xyzf[0], e_y=energy_xyzf[1],
                e_z=energy_xyzf[2], e_f=energy_xyzf[3])
            self.connection.cursor().execute(query)
            self.connection.commit()

    def get_pre_correction_data(
            self) -> List[Tuple[int, float, float]]:
        query = 'SELECT * FROM pre_correction;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def clear_corrections(self):
        query = 'DELETE FROM corrections;'
        self.connection.cursor().execute(query)
        self.connection.commit()

    def add_single_correction(self, minute_id: int, seis_correction: float):
        query = 'INSERT INTO corrections(minute_id, seis_corr) VALUES ' \
                f'({minute_id}, {seis_correction});'
        self.connection.cursor().execute(query)
        self.connection.commit()
        self.logger.debug(f'Seismic correction with minute_id={minute_id} '
                          f'value={seis_correction} added')

    def get_all_chain_ids(self) -> List[int]:
        query = 'SELECT id FROM chains;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        ids_list = [x[0] for x in cursor.fetchall()]
        return ids_list

    def get_links_by_chain_id(self, chain_id: int) -> List[int]:
        query = f'SELECT id FROM links AS l WHERE l.chain_id={chain_id} ' \
                f'ORDER BY l.link_id ASC;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return [x[0] for x in records]

    def get_device_pairs_by_chain_id(
            self, chain_id: int) -> List[Tuple[int, int]]:
        query = 'SELECT DISTINCT gravimeter_id, seismometer_id ' \
                f'FROM sensor_pairs WHERE chain_id={chain_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        if not records:
            return []
        return records

    def is_sensor_pair_exists(self, chain_id: int, link_id: int,
                              gravimeter_id: int,
                              seismometer_id: int) -> bool:
        query = 'SELECT COUNT(1) FROM sensor_pairs WHERE ' \
                f'chain_id={chain_id} AND link_id={link_id} AND ' \
                f'gravimeter_id={gravimeter_id} AND ' \
                f'seismometer_id={seismometer_id}'
        cursor = self.connection.cursor()
        cursor.execute(query)
        if cursor.fetchone()[0]:
            return True
        return False

    def get_gravity_measures_count_by_link_id(self, link_id: int) -> int:
        query = 'SELECT COUNT(1) FROM gravity_measures ' \
                'WHERE dat_file_id=(SELECT id FROM dat_files ' \
                f'WHERE link_id={link_id})'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_gravity_measures_by_file_id(self, id_val: int) -> List[float]:
        query = 'SELECT corr_grav FROM gravity_measures ' \
                f'WHERE dat_file_id={id_val};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

    def get_gravity_level_by_time_intersection_id(self, id_val: int) -> float:
        query = 'SELECT avg_grav FROM grav_level ' \
                f'WHERE time_intersection_id={id_val};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_session_index(self, chain_id: int, link_id: int) -> int:
        query = 'SELECT link_id FROM links ' \
                f'WHERE links.chain_id={chain_id} AND links.id={link_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def is_chain_has_corrections(
            self, chain_id: int, gravimeter_id: int,
            seismometer_id: int) -> bool:
        query = 'SELECT COUNT(1) FROM sensor_pairs AS sp WHERE ' \
                f'sp.chain_id={chain_id} AND ' \
                f'sp.gravimeter_id={gravimeter_id} AND ' \
                f'sp.seismometer_id={seismometer_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return True if cursor.fetchone()[0] else False

    def get_post_corrections_by_params(
            self, chain_id: int, link_id: int, gravimeter_id: int,
            seismometer_id: int) -> List[tuple]:
        query = 'SELECT cycle, seis_corr FROM post_correction AS pc ' \
                f'WHERE pc.chain_id={chain_id} AND pc.link_id={link_id} ' \
                f'AND pc.gravimeter_id={gravimeter_id} AND ' \
                f'pc.seismometer_id={seismometer_id} ORDER BY cycle;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records

    def get_chain_datetime_by_id(self, chain_id: int) -> datetime:
        query = 'SELECT MIN(datetime_start) FROM dat_files as df ' \
                'WHERE df.link_id IN (SELECT id FROM links as l ' \
                f'WHERE l.chain_id={chain_id});'
        cursor = self.connection.cursor()
        cursor.execute(query)
        datetime_str = cursor.fetchone()[0]
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

    def get_gravimeter_short_number_by_id(self, gravimeter_id: int) -> str:
        query = 'SELECT substr(number, -4) FROM gravimeters AS g ' \
                f'WHERE g.id={gravimeter_id};'

        cursor = self.connection.cursor()
        cursor.execute(query)
        short_number = cursor.fetchone()[0]
        return short_number

    def get_seismometer_number_by_id(self, seismometer_id: int) -> str:
        query = 'SELECT number FROM seismometers AS s ' \
            f'WHERE s.id={seismometer_id};'

        cursor = self.connection.cursor()
        cursor.execute(query)
        number = cursor.fetchone()[0]
        return number

    def get_chain_ids_by_stations(self, stations: List[str]) -> List[int]:
        stations_str = ', '.join((f'\'{x}\'' for x in stations))
        query = 'SELECT DISTINCT l.chain_id FROM time_intersection AS ti' \
            'JOIN dat_files AS df ON ti.grav_dat_id=df.id' \
            'JOIN links AS l ON l.id=df.link_id' \
            'WHERE df.station_id IN (SELECT id FROM stations as s ' \
            f'WHERE name IN ({stations_str})) ORDER BY chain_id ASC;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return [x[0] for x in records]

    def get_seis_energy_by_time_intersection_id(
            self, id_val: int) -> List[Tuple[float, float, float, float]]:
        query = 'SELECT Ex, Ey, Ez, Efull FROM seis_energy ' \
            'WHERE minute_id IN (SELECT id FROM minutes_intersection ' \
            f'WHERE time_intersection_id={id_val});'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def get_seis_corrections_by_time_intersection_id(
            self, id_val: int) -> List[float]:
        query = 'SELECT seis_corr FROM corrections ' \
            'WHERE minute_id IN (SELECT id FROM minutes_intersection ' \
            f'WHERE time_intersection_id={id_val});'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

    def get_start_datetime_gravity_measures_by_time_intersection_id(
            self, id_val: int) -> datetime:
        query = 'SELECT datetime_start FROM dat_files ' \
                'WHERE id=(SELECT grav_dat_id FROM time_intersection ' \
                f'WHERE id={id_val});'
        cursor = self.connection.cursor()
        cursor.execute(query)

        record = cursor.fetchone()[0]
        return datetime.strptime(record, '%Y-%m-%d %H:%M:%S')

    def get_start_datetime_intersection_info_by_id(
            self, id_val: int) -> datetime:
        query = 'SELECT datetime_start FROM time_intersection ' \
                f'WHERE id={id_val};'
        cursor = self.connection.cursor()
        cursor.execute(query)

        record = cursor.fetchone()[0]
        return datetime.strptime(record, '%Y-%m-%d %H:%M:%S')
