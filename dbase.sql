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
    name VARCHAR(20) UNIQUE NOT NULL,
    xWGS84 REAL NOT NULL DEFAULT 0,
    yWGS84 REAL NOT NULL DEFAULT 0
);

CREATE TABLE chains(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    dev_num_part VARCHAR(10) NOT NULL,
    path TEXT UNIQUE NOT NULL
);

CREATE TABLE links(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    chain_id INTEGER NOT NULL,
    link_index INTEGER NOT NULL,
    filename VARCHAR(100) NOT NULL,
    is_exist int DEFAULT 0,
    UNIQUE(chain_id, link_index),
    FOREIGN KEY (chain_id) REFERENCES chains(id)
);

CREATE TABLE grav_dat_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    gravimeter_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    filename VARCHAR(100) UNIQUE NOT NULL,
    path TEXT UNIQUE NOT NULL,
    FOREIGN KEY(gravimeter_id) REFERENCES gravimeters(id),
    FOREIGN KEY(station_id) REFERENCES stations(id)
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
    is_bad INTEGER NOT NULL DEFAULT 0,
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

CREATE TABLE seis_files_defect_info(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seis_id INTEGER NOT NULL,
    x_channel VARCHAR(10) CHECK(x_channel IN ('Good', 'Bad', 'Unknown')) DEFAULT 'Unknown',
    y_channel VARCHAR(10) CHECK(x_channel IN ('Good', 'Bad', 'Unknown')) DEFAULT 'Unknown',
    z_channel VARCHAR(10) CHECK(x_channel IN ('Good', 'Bad', 'Unknown')) DEFAULT 'Unknown',
    FOREIGN KEY (seis_id) REFERENCES seis_files(id)
);

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

CREATE TABLE corrections(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    minute_id INTEGER NOT NULL,
    seis_corr REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (minute_id) REFERENCES minutes_intersection(id) ON DELETE CASCADE
);


CREATE VIEW need_check_seis_files
AS
SELECT sf.id, sf.path, di.x_channel, di.y_channel, di.z_channel
FROM seis_files AS sf
JOIN seis_files_defect_info AS di ON sf.id=di.seis_id
WHERE (di.x_channel='Unknown' OR di.y_channel='Unknown' OR di.z_channel='Unknown') AND (di.x_channel!='Bad' AND di.y_channel!='Bad'
AND di.z_channel!='Bad');

CREATE VIEW good_seis_files
AS
SELECT sf.id
FROM seis_files AS sf
JOIN seis_files_defect_info AS di ON sf.id=di.seis_id
WHERE di.x_channel NOT IN ('Bad', 'Unknown') AND di.y_channel NOT IN ('Bad', 'Unknown') AND di.z_channel NOT IN ('Bad', 'Unknown');

CREATE VIEW grav_seis_times
AS
SELECT g.id AS grav_id, s.id AS seis_id, g.datetime_start AS grav_dt_start,
g.datetime_stop AS grav_dt_stop, s.datetime_start AS seis_datetime_start,
s.datetime_stop AS seis_datetime_stop
FROM dat_files AS g JOIN seis_files AS s ON g.station_id = s.station_id AND
MAX(g.datetime_start, s.datetime_start) < MIN(g.datetime_stop, s.datetime_stop)
JOIN good_seis_files AS gsf ON s.id=gsf.id;

CREATE VIEW minimal_energy
AS
SELECT mi.time_intersection_id, mi.id AS minute_id, Ez
FROM seis_energy AS se
JOIN minutes_intersection AS mi ON se.minute_id=mi.id
GROUP BY time_intersection_id
HAVING Efull = MIN(EFull);

CREATE VIEW energy_ratio
AS
SELECT se.minute_id, se.Ez/me.eZ AS energy_ratio FROM seis_energy AS se
JOIN minutes_intersection AS mi ON se.minute_id=mi.id
JOIN minimal_energy AS me ON me.time_intersection_id=mi.time_intersection_id;

CREATE VIEW grav_level
AS
SELECT ti.id AS time_intersection_id, gm.corr_grav AS avg_grav
FROM minimal_energy AS me
JOIN time_intersection AS ti ON me.time_intersection_id=ti.id
JOIN minutes_intersection AS mi ON me.minute_id=mi.id
JOIN gravity_measures AS gm ON gm.dat_file_id=ti.grav_dat_id
WHERE gm.datetime_val=datetime(strftime('%s', ti.datetime_start)+(mi.minute_index + 1) * 60, 'unixepoch');

CREATE VIEW pre_correction
AS
SELECT mi.id AS minute_id, round(gl.avg_grav-gm.corr_grav, 4) AS amplitude, er.energy_ratio
FROM minutes_intersection AS mi
JOIN time_intersection AS ti ON mi.time_intersection_id=ti.id
JOIN gravity_measures AS gm ON gm.dat_file_id=ti.grav_dat_id
JOIN grav_level AS gl ON gl.time_intersection_id=ti.id
JOIN energy_ratio AS er ON er.minute_id=mi.id
WHERE gm.datetime_val=datetime(strftime('%s', ti.datetime_start)+(mi.minute_index + 1) * 60, 'unixepoch');

CREATE VIEW using_seis_files
AS
SELECT sf.id AS file_id, sf.path, ti.datetime_start, ti.datetime_stop
FROM time_intersection AS ti
JOIN seis_files AS sf ON ti.seis_id=sf.id;

CREATE VIEW sensor_pairs
AS
SELECT distinct c.id AS chain_id, l.id AS link_id, df.gravimeter_id, sf.sensor_id AS seismometer_id FROM chains AS c
LEFT JOIN links AS l on c.id=l.chain_id
LEFT JOIN dat_files AS df ON df.link_id=l.id
LEFT JOIN time_intersection AS ti ON ti.grav_dat_id=df.id
LEFT JOIN seis_files AS sf ON sf.id=ti.seis_id
WHERE seismometer_id IS NOT NULL;

CREATE VIEW post_correction
AS
SELECT l.chain_id, l.id AS link_id, ti.id AS time_intersection_id, s.id AS seismometer_id, g.id AS gravimeter_id, gm.id - (SELECT MIN(id) FROM 
gravity_measures AS gm WHERE gm.dat_file_id=df.id) + 1 AS cycle, c.seis_corr AS seis_corr from links AS l
JOIN dat_files AS df ON df.link_id=l.id
JOIN gravimeters AS g ON g.id=df.gravimeter_id
JOIN gravity_measures AS gm ON gm.dat_file_id=df.id
LEFT JOIN time_intersection AS ti ON ti.grav_dat_id=df.id
LEFT JOIN minutes_intersection AS mi ON mi.time_intersection_id=ti.id AND gm.datetime_val=datetime(strftime('%s', ti.datetime_start)+(mi.minute_index + 1) * 60, 'unixepoch')
LEFT JOIN corrections AS c ON c.minute_id=mi.id
LEFT JOIN seis_files AS sf ON sf.id=ti.seis_id
LEFT JOIN seismometers AS s ON s.id=sf.sensor_id
ORDER BY chain_id ASC, l.link_id ASC, gm.id ASC, ti.id ASC;

CREATE VIEW pre_plotting
AS
SELECT sp.chain_id, ti.id AS time_intersection_id, sp.gravimeter_id, sp.seismometer_id, ti.grav_dat_id AS dat_file_id, tf.id AS tsf_file_id, ti.seis_id AS seis_file_id FROM sensor_pairs AS sp
JOIN dat_files AS df ON sp.link_id=df.link_id
JOIN time_intersection AS ti ON ti.grav_dat_id=df.id
JOIN gravimeters AS g ON g.id=df.gravimeter_id
LEFT JOIN tsf_files AS tf ON tf.dev_num_part=substr(g.number, -4) AND tf.datetime_start < df.datetime_stop AND df.datetime_stop <= tf.datetime_stop;
