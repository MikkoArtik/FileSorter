import os
import sqlite3
from datetime import datetime
from datetime import timedelta
from typing import Union, List, Tuple, Dict
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

    def add_chain(self, sensor_part_name: str,
                  chain_path: str, cycle_path: str) -> int:
        query = 'INSERT INTO chains(dev_num_part, chain_path, cycle_path) ' \
                f'VALUES (\'{sensor_part_name}\', \'{chain_path}\', ' \
                f'\'{cycle_path}\')'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new chain with path {chain_path} successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new chain with path {chain_path} failed')

        query = f'SELECT id FROM chains WHERE chain_path=\'{chain_path}\''
        id_val = cursor.execute(query).fetchone()[0]
        self.logger.info(f'chain: path={chain_path} '
                         f'sensor_part_name={sensor_part_name} id={id_val}')
        return id_val

    def add_link(self, chain_id: int, order: int, filename: str) -> int:
        query = 'INSERT INTO links(chain_id, link_index, filename) VALUES ' \
                f'({chain_id}, {order}, \'{filename}\');'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(f'insert new link {filename} successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'insert new link {filename} failed')

        query = f'SELECT id FROM links ' \
                f'WHERE filename=\'{filename}\' AND chain_id={chain_id};'
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

    def change_link_status(self, grav_dat_filename: str, is_exist=True):
        query = f'UPDATE links SET is_exist={int(is_exist)} ' \
                f'WHERE filename=\'{grav_dat_filename}\';'
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            self.logger.debug(
                f'status for link with filename={grav_dat_filename} changed '
                f'to {is_exist}')
        except sqlite3.IntegrityError:
            self.logger.error(
                f'status for link with filename={grav_dat_filename} '
                'not change')

    def get_id_grav_dat_file_by_path(self, path: str) -> Union[int, None]:
        query = f'SELECT id FROM grav_dat_files WHERE path=\'{path}\''
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchone()
        if not records:
            return None
        else:
            return records[0]

    def add_grav_dat_file(self, grav_number: str, station: str,
                          datetime_start: datetime, datetime_stop: datetime,
                          path: str):
        filename = os.path.basename(path)
        sensor_id = self.add_gravimeter(grav_number)
        point_id = self.add_station(station)

        query = 'INSERT INTO grav_dat_files(gravimeter_id, station_id, ' \
                'datetime_start, datetime_stop, filename, path) VALUES ' \
                f'({sensor_id}, {point_id}, \'{datetime_start}\', ' \
                f'\'{datetime_stop}\', \'{filename}\',\'{path}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'DAT-file with path {path} added successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'DAT-file with path {path} not add to dbase')

    def add_gravity_minute_measures(self, dat_file_id: int,
                                    measures: List[Tuple[datetime, int]]):
        query_template = 'INSERT INTO gravity_measures_minutes (' \
                         'grav_dat_file_id, datetime_val, corr_grav) ' \
                         'VALUES ({dat_file_id}, \'{datetime_val}\', ' \
                         '{corr_grav});'

        cursor = self.connection.cursor()
        for datetime_val, corr_grav in measures:
            query = query_template.format(dat_file_id=dat_file_id,
                                          datetime_val=datetime_val,
                                          corr_grav=corr_grav)
            cursor.execute(query)
        self.connection.commit()

    def add_grav_tsf_file(self, dev_num_part: str, datetime_start: datetime,
                          datetime_stop: datetime, path: str):
        query = 'INSERT INTO grav_tsf_files(dev_num_part, datetime_start, ' \
                                           'datetime_stop, path) ' \
                f'VALUES (\'{dev_num_part}\', \'{datetime_start}\', ' \
                f'\'{datetime_stop}\', \'{path}\')'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'TSF-file with path {path} added successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'TSF-file with path {path} not add')

    def get_id_grav_tsf_file_by_path(self, path: str) -> int:
        query = f'SELECT id FROM grav_tsf_files WHERE path=\'{path}\';'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def add_gravity_second_measures(self, tsf_file_id: int,
                                    measures: List[Tuple[int, int]]):
        query_template = 'INSERT INTO gravity_measures_seconds (' \
                'grav_tsf_file_id, measure_index, src_value) VALUES ' \
                '({tsf_file_id}, \'{measure_index}\', {src_value});'
        cursor = self.connection.cursor()
        for measure_index, value in enumerate(measures):
            datetime_val, src_value = value
            query = query_template.format(tsf_file_id=tsf_file_id,
                                          measure_index=measure_index,
                                          src_value=src_value)
            cursor.execute(query)
        self.connection.commit()

    def get_grav_defect_input_preparing(self) -> List[Tuple[int, int, str]]:
        query = 'SELECT * FROM grav_defect_input_preparing;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def update_grav_defect_marker(self, grav_dat_file_id: int,
                                  cycle_index: int, is_bad: bool):
        is_bad_val = 1 if is_bad else 0
        query = f'UPDATE gravity_measures_minutes SET is_bad={is_bad_val} ' \
            f'WHERE grav_dat_file_id={grav_dat_file_id} AND ' \
            f'datetime_val=datetime(strftime(\'%s\', ' \
                f'(SELECT datetime_start FROM grav_dat_files ' \
                f'WHERE id={grav_dat_file_id}))+ {cycle_index} * 60,' \
                f'\'unixepoch\');'
        self.connection.cursor().execute(query)

    def update_grav_defect_markers(self, grav_dat_file_id: int,
                                   markers: Dict[int, bool]):
        for cycle_index, is_bad in markers.items():
            self.update_grav_defect_marker(grav_dat_file_id, cycle_index,
                                           is_bad)
        self.connection.commit()

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

    def update_seis_file_checking_status(self, file_id: int, component: str,
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

    def clear_grav_seis_time_intersections(self):
        query = 'DELETE FROM grav_seis_time_intersections;'
        self.connection.cursor().execute(query)
        self.connection.commit()

    def get_grav_seis_pairs(self):
        query = 'SELECT * FROM grav_seis_pairs;'
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

    def add_grav_seis_time_intersection(self, grav_dat_id: int, seis_id,
                                        datetime_left: datetime,
                                        datetime_right: datetime):
        query = 'INSERT INTO grav_seis_time_intersections(grav_dat_id, ' \
                'seis_id, datetime_start, datetime_stop) VALUES (' \
                f'{grav_dat_id}, {seis_id}, \'{datetime_left}\', ' \
                f'\'{datetime_right}\');'
        try:
            self.connection.cursor().execute(query)
            self.connection.commit()
            self.logger.debug(f'Time intersection added successful')
        except sqlite3.IntegrityError:
            self.logger.error(f'Fail adding time intersection')

    def get_grav_seis_time_intersections(self) -> List[Tuple[int, int, int,
                                                             datetime, datetime]]:
        query = 'SELECT * FROM grav_seis_time_intersections;'
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

    def delete_all_energies(self):
        query = 'DELETE FROM seis_energy;'
        self.connection.cursor().execute(query)
        self.connection.commit()

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

    def add_energies(self, time_intersection_id: int,
                     energies: List[List[float]]):
        query_template = 'INSERT INTO seis_energy(time_intersection_id, ' \
                         'minute_index, Ex, Ey, Ez, Efull) ' \
                         'VALUES ({time_intersection_id}, {minute_index}, ' \
                         '{e_x}, {e_y}, {e_z}, {e_f});'
        cursor = self.connection.cursor()
        for index, energy_xyzf in enumerate(energies):
            query = query_template.format(
                time_intersection_id=time_intersection_id,
                minute_index=index, e_x=energy_xyzf[0], e_y=energy_xyzf[1],
                e_z=energy_xyzf[2], e_f=energy_xyzf[3])
            cursor.execute(query)
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

    def add_single_correction(self, time_intersection_id: int,
                              grav_measure_id: int, seis_correction: float):
        query = 'INSERT INTO corrections(time_intersection_id, ' \
                'grav_measure_id, seis_corr) VALUES (' \
                f'{time_intersection_id}, {grav_measure_id}, {seis_correction});'
        self.connection.cursor().execute(query)

    def add_seis_corrections(self, time_intersection_id: int,
                             corrections: List[Tuple[int, float]]):
        for grav_measure_id, correction_val in corrections:
            self.add_single_correction(time_intersection_id, grav_measure_id,
                                       correction_val)
        self.connection.commit()

    def get_all_chain_ids(self) -> List[int]:
        query = 'SELECT id FROM chains;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        ids_list = [x[0] for x in cursor.fetchall()]
        return ids_list

    def get_links_by_chain_id(self, chain_id: int) -> List[int]:
        query = f'SELECT id FROM links WHERE chain_id={chain_id} ' \
                f'ORDER BY link_index ASC;'
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
                f'seismometer_id={seismometer_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        if cursor.fetchone()[0]:
            return True
        return False

    def get_gravity_defect_info_by_link_id(self, link_id: int) -> List[int]:
        query = 'SELECT is_bad FROM gravity_measures_minutes ' \
                'WHERE grav_dat_file_id=(SELECT id FROM grav_dat_files ' \
                'WHERE filename=(SELECT filename FROM links WHERE ' \
                f'id={link_id}));'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

    def get_link_index(self, chain_id: int, link_id: int) -> int:
        query = 'SELECT link_index FROM links ' \
                f'WHERE links.chain_id={chain_id} AND links.id={link_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_post_corrections_by_params(
            self, chain_id: int, link_id: int, gravimeter_id: int,
            seismometer_id: int) -> List[tuple]:
        query = 'SELECT cycle_index, is_bad, seis_corr ' \
                'FROM post_correction ' \
                f'WHERE chain_id={chain_id} AND link_id={link_id} AND ' \
                f'time_intersection_id=(SELECT time_intersection_id FROM ' \
                f'sensor_pairs AS sp WHERE sp.chain_id={chain_id} AND ' \
                f'sp.link_id={link_id} AND ' \
                f'sp.seismometer_id={seismometer_id} AND ' \
                f'sp.gravimeter_id={gravimeter_id}) ORDER BY cycle_index;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records

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

    def get_correction_filename(self, chain_id: int) -> str:
        query = f'SELECT cycle_path FROM chains WHERE id={chain_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        filename = os.path.basename(cursor.fetchone()[0])
        return filename

    def get_chain_datetime_by_id(self, chain_id: int) -> datetime:
        query = 'SELECT MIN(datetime_start) FROM grav_dat_files as df ' \
                'WHERE filename=(SELECT filename FROM links ' \
                f'WHERE chain_id={chain_id});'
        cursor = self.connection.cursor()
        cursor.execute(query)
        datetime_str = cursor.fetchone()[0]
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

    def get_gravimeter_short_number_by_id(self, gravimeter_id: int) -> str:
        query = f'SELECT number FROM gravimeters WHERE id={gravimeter_id};'

        cursor = self.connection.cursor()
        cursor.execute(query)
        short_number = str(int(cursor.fetchone()[0]))
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

    def get_grav_minute_measures_by_ti_id(
            self, ti_id: int) -> List[Tuple[datetime, float, bool]]:
        cursor = self.connection.cursor()

        query = 'SELECT datetime_val, corr_grav, is_bad ' \
                'FROM gravity_measures_minutes ' \
                'WHERE grav_dat_file_id=(' \
                '   SELECT grav_dat_id ' \
                '   FROM grav_seis_time_intersections' \
                f'   WHERE id={ti_id});'

        cursor.execute(query)
        result = []
        for rec in cursor.fetchall():
            dt_val = datetime.strptime(rec[0], '%Y-%m-%d %H:%M:%S')
            is_bad = True if rec[2] else False
            result.append((dt_val, rec[1], is_bad))
        return result


    def get_seis_corrections_by_ti_id(self, ti_id: int) -> List[float]:
        cursor = self.connection.cursor()

        query = 'SELECT id ' \
                'FROM gravity_measures_minutes ' \
                'WHERE grav_dat_file_id=(' \
                '   SELECT grav_dat_id ' \
                '   FROM grav_seis_time_intersections' \
                f'   WHERE id={ti_id});'
        cursor.execute(query)
        result = dict()
        for rec in cursor.fetchall():
            id_val = rec[0]
            result[id_val] = 0

        query = 'SELECT grav_measure_id, seis_corr ' \
                'FROM corrections ' \
                f'WHERE time_intersection_id={ti_id};'
        cursor.execute(query)
        for rec in cursor.fetchall():
            id_val, corr_val = rec
            result[id_val] = corr_val

        return [x[1] for x in
                sorted(list(result.items()), key=lambda x: x[0])]

    def get_grav_level_by_ti_id(self, ti_id: int) -> float:
        cursor = self.connection.cursor()

        query = 'SELECT quite_grav_level ' \
                'FROM grav_level ' \
                f'WHERE time_intersection_id={ti_id};'
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_seis_energy_by_ti_id(self,
                                 ti_id: int) -> List[Tuple[datetime, float]]:
        cursor = self.connection.cursor()

        query = 'SELECT datetime_start ' \
                'FROM grav_seis_time_intersections ' \
                f'WHERE id={ti_id};'
        cursor.execute(query)

        datetime_start = datetime.strptime(cursor.fetchone()[0],
                                           '%Y-%m-%d %H:%M:%S')

        query = 'SELECT minute_index, Ez ' \
                'FROM seis_energy ' \
                f'WHERE time_intersection_id={ti_id} ' \
                'ORDER BY minute_index ASC;'
        cursor.execute(query)

        energy_vals = []
        for minute_index, e_z in cursor.fetchall():
            datetime_val = datetime_start + timedelta(
                minutes=minute_index + 1)
            energy_vals.append((datetime_val, e_z))
        return energy_vals

    def get_seis_level_by_ti_id(self, ti_id: int) -> float:
        cursor = self.connection.cursor()

        query = 'SELECT Ez ' \
                'FROM minimal_energy ' \
                f'WHERE time_intersection_id={ti_id};'
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_tsf_file_path_by_ti_id(self, ti_id: int) -> str:
        cursor = self.connection.cursor()

        query = 'SELECT gtf.path ' \
                'FROM grav_tsf_files AS gtf ' \
                'JOIN gravimeters AS g ON SUBSTR(g.number, -4)=gtf.dev_num_part ' \
                'JOIN grav_dat_files AS gdf ON gtf.datetime_start < gdf.datetime_start AND gdf.datetime_stop <= gtf.datetime_stop AND gdf.gravimeter_id=g.id ' \
                'JOIN grav_seis_time_intersections AS ti ON ti.grav_dat_id=gdf.id ' \
                f'WHERE ti.id={ti_id};'
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_seis_file_path_by_ti_id(self, ti_id: int) -> str:
        cursor = self.connection.cursor()

        query = 'SELECT sf.path ' \
                'FROM grav_seis_time_intersections AS ti ' \
                'JOIN seis_files AS sf ON sf.id=ti.seis_id ' \
                f'WHERE ti.id={ti_id};'
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_quite_minute_start_by_ti_id(self, ti_id: int) -> datetime:
        cursor = self.connection.cursor()

        query = 'SELECT minute_index ' \
                'FROM minimal_energy ' \
                f'WHERE time_intersection_id={ti_id};'
        cursor.execute(query)
        minute_index = cursor.fetchone()[0]

        query = 'SELECT datetime_start ' \
                'FROM grav_seis_time_intersections ' \
                f'WHERE id={ti_id};'
        cursor.execute(query)
        datetime_val = datetime.strptime(cursor.fetchone()[0],
                                         '%Y-%m-%d %H:%M:%S')
        return datetime_val + timedelta(minutes=minute_index)

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

    def get_tsf_file_path_by_id(self, id_val: int) -> str:
        query = f'SELECT path FROM tsf_files WHERE id={id_val};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def get_sensor_pair_info(self, ti_id: int) -> Tuple[str, str, str, str]:
        query = 'SELECT st.name, g.number, s.number, sf.datetime_start ' \
                'FROM grav_seis_time_intersections AS ti ' \
                'JOIN seis_files AS sf ON sf.id=ti.seis_id ' \
                'JOIN seismometers AS s ON s.id=sf.sensor_id ' \
                'JOIN stations AS st ON st.id=sf.station_id ' \
                'JOIN grav_dat_files AS gdf ON gdf.id=ti.grav_dat_id ' \
                'JOIN gravimeters AS g ON g.id=gdf.gravimeter_id '\
                f'WHERE ti.id={ti_id};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        record = list(cursor.fetchone())
        record[1] = str(int(record[1]))

        datetime_val = datetime.strptime(record[3], '%Y-%m-%d %H:%M:%S')
        date_str = datetime_val.strftime('%d.%m.%Y')
        record[3] = date_str

        return tuple(record)
