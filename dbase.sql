CREATE TABLE chains(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    dev_num_part VARCHAR(10) NOT NULL,
    path TEXT UNIQUE NOT NULL
);

CREATE TABLE links(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    chain_id INTEGER NOT NULL,
    link_id INTEGER NOT NULL,
    filename VARCHAR(100) UNIQUE NOT NULL,
    is_exist int DEFAULT 0,
    FOREIGN KEY (chain_id) REFERENCES chains(id)
);

CREATE TABLE gravimeters(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    number VARCHAR(15) UNIQUE NOT NULL
);

CREATE TABLE seismometers(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    number VARCHAR(15) UNIQUE NOT NULL
);

CREATE TABLE stations(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    name VARCHAR(20) UNIQUE NOT NULL
);

CREATE TABLE dat_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    gravimeter_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    link_id INTEGER NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    path TEXT UNIQUE NOT NULL,
    FOREIGN KEY(gravimeter_id) REFERENCES gravimeters(id),
    FOREIGN KEY(station_id) REFERENCES stations(id),
    FOREIGN KEY(link_id) REFERENCES links(id)
);

CREATE TABLE tsf_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    dev_num_part VARCHAR(10) NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    path TEXT UNIQUE NOT NULL);

CREATE TABLE seis_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    sensor_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    path TEXT UNIQUE NOT NULL,
    FOREIGN KEY(sensor_id) REFERENCES seismometers(id),
    FOREIGN KEY(station_id) REFERENCES stations(id));


CREATE VIEW grav_file_pairs
AS
SELECT s.name, g.number AS grav_sensor, d.datetime_start, d.datetime_stop, d.path AS dat_path, t.path AS tsf_path
FROM dat_files AS d
JOIN stations AS s ON d.station_id=s.id
JOIN gravimeters AS g ON d.gravimeter_id=g.id
JOIN tsf_files AS t ON d.datetime_stop=t.datetime_stop AND substr(g.number, -4)=t.dev_num_part;


CREATE VIEW grav_seis_pairs
AS
SELECT g.name AS point, g.grav_sensor, ss.number AS seis_sensor, g.datetime_start, g.datetime_stop, g.dat_path, g.tsf_path, s.path AS seis_path
FROM grav_file_pairs AS g
JOIN seis_files AS s ON
s.station_id=(SELECT id FROM stations WHERE name=g.name) AND MAX(g.datetime_start, s.datetime_start) < MIN(g.datetime_stop, s.datetime_stop)
JOIN seismometers AS ss ON s.sensor_id=ss.id;