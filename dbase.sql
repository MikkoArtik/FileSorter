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
    UNIQUE(chain_id, link_id),
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

CREATE TABLE gravity_measures(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dat_file_id INTEGER NOT NULL,
    datetime_val DATETIME NOT NULL,
    corr_grav REAL NOT NULL DEFAULT 0,
    UNIQUE(dat_file_id, datetime_val),
    FOREIGN KEY (dat_file_id) REFERENCES dat_files(id) ON DELETE CASCADE
);

CREATE TABLE seis_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    sensor_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    path TEXT UNIQUE NOT NULL,
    FOREIGN KEY(sensor_id) REFERENCES seismometers(id),
    FOREIGN KEY(station_id) REFERENCES stations(id));

CREATE TABLE time_intersection(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grav_dat_id INTEGER NOT NULL,
    seis_id INTEGER NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    FOREIGN KEY(grav_dat_id) REFERENCES dat_files(id),
    FOREIGN KEY(seis_id) REFERENCES seis_files(id)
);

CREATE TABLE minutes_intersection(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_intersection_id INTEGER NOT NULL,
    minute_index INTEGER NOT NULL,
    UNIQUE(time_intersection_id, minute_index),
    FOREIGN KEY (time_intersection_id) REFERENCES time_intersection(id) ON DELETE CASCADE
);

CREATE TABLE seis_energy(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    minute_id INTEGER NOT NULL,
    Ex REAL NOT NULL DEFAULT 0,
    Ey REAL NOT NULL DEFAULT 0,
    Ez REAL NOT NULL DEFAULT 0,
    Efull REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (minute_id) REFERENCES minutes_intersection(id) ON DELETE CASCADE
);


CREATE VIEW grav_seis_times
AS
SELECT g.id AS grav_id, s.id AS seis_id, g.datetime_start AS grav_dt_start,
g.datetime_stop AS grav_dt_stop, s.datetime_start AS seis_datetime_start,
s.datetime_stop AS seis_datetime_stop
FROM dat_files AS g JOIN seis_files AS s ON g.station_id = s.station_id AND
MAX(g.datetime_start, s.datetime_start) < MIN(g.datetime_stop, s.datetime_stop);

CREATE VIEW minimal_energy
AS
SELECT mi.time_intersection_id, mi.id AS minute_id, Ez
FROM seis_energy AS se
JOIN minutes_intersection AS mi ON se.minute_id=mi.id
GROUP BY time_intersection_id
HAVING Efull = MIN(EFull);

CREATE VIEW energy_ratio
AS
SELECT se.minute_id, se.Ez/me.eZ as energy_ratio FROM seis_energy as se
JOIN minutes_intersection as mi ON se.minute_id=mi.id
JOIN minimal_energy as me ON me.time_intersection_id=mi.time_intersection_id;
