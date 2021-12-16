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
    chain_path TEXT UNIQUE NOT NULL,
    cycle_path TEXT UNIQUE NOT NULL
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

CREATE TABLE gravity_measures_minutes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grav_dat_file_id INTEGER NOT NULL,
    datetime_val DATETIME NOT NULL,
    corr_grav REAL NOT NULL DEFAULT 0,
    is_bad INTEGER NOT NULL DEFAULT 0,
    UNIQUE(grav_dat_file_id, datetime_val),
    FOREIGN KEY (grav_dat_file_id) REFERENCES grav_dat_files(id) ON DELETE CASCADE
);

CREATE TABLE grav_tsf_files(
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    dev_num_part VARCHAR(10) NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    path TEXT UNIQUE NOT NULL
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

CREATE TABLE grav_seis_time_intersections(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grav_dat_id INTEGER NOT NULL,
    seis_id INTEGER NOT NULL,
    datetime_start DATETIME NOT NULL,
    datetime_stop DATETIME NOT NULL,
    FOREIGN KEY(grav_dat_id) REFERENCES dat_files(id),
    FOREIGN KEY(seis_id) REFERENCES seis_files(id)
);

CREATE TABLE seis_energy(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_intersection_id INTEGER NOT NULL,
    minute_index INTEGER NOT NULL,
    Ex REAL NOT NULL DEFAULT 0,
    Ey REAL NOT NULL DEFAULT 0,
    Ez REAL NOT NULL DEFAULT 0,
    Efull REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (time_intersection_id) REFERENCES time_intersection(id) ON DELETE CASCADE
);

CREATE TABLE median_energy(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_intersection_id INTEGER NOT NULL,
    Ex REAL NOT NULL DEFAULT 0,
    Ey REAL NOT NULL DEFAULT 0,
    Ez REAL NOT NULL DEFAULT 0,
    Efull REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (time_intersection_id) REFERENCES time_intersection(id) ON DELETE CASCADE
);

CREATE TABLE corrections(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_intersection_id INTEGER NOT NULL,
    grav_measure_id INTEGER NOT NULL,
    seis_corr REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (time_intersection_id) REFERENCES grav_seis_time_intersections (id) ON DELETE CASCADE
    FOREIGN KEY (grav_measure_id) REFERENCES gravity_measures_minutes(id) ON DELETE CASCADE
);


CREATE VIEW need_check_seis_files
AS
SELECT sf.id, sf.path, di.x_channel, di.y_channel, di.z_channel
FROM seis_files AS sf
JOIN seis_files_defect_info AS di ON sf.id=di.seis_id
WHERE (di.x_channel='Unknown' OR di.y_channel='Unknown' OR di.z_channel='Unknown') AND
      (di.x_channel!='Bad' AND di.y_channel!='Bad' AND di.z_channel!='Bad');

CREATE VIEW good_seis_files
AS
SELECT sf.id
FROM seis_files AS sf
JOIN seis_files_defect_info AS di ON sf.id=di.seis_id
WHERE di.x_channel NOT IN ('Bad', 'Unknown') AND
      di.y_channel NOT IN ('Bad', 'Unknown') AND
      di.z_channel NOT IN ('Bad', 'Unknown');

CREATE VIEW grav_defect_input_preparing
AS
SELECT gdf.id as grav_dat_file_id, l.link_index, c.cycle_path FROM links AS l
LEFT JOIN grav_dat_files AS gdf ON l.filename=gdf.filename
LEFT JOIN chains AS c ON c.id=l.chain_id
WHERE c.id NOT IN (SELECT DISTINCT chain_id
                   FROM links
                   WHERE is_exist=0);

CREATE VIEW grav_seis_pairs
AS
SELECT g.id AS grav_id, s.id AS seis_id, g.datetime_start AS grav_dt_start,
g.datetime_stop AS grav_dt_stop, s.datetime_start AS seis_datetime_start,
s.datetime_stop AS seis_datetime_stop
FROM grav_dat_files AS g
JOIN seis_files AS s ON g.station_id = s.station_id AND
    MAX(g.datetime_start, s.datetime_start) < MIN(g.datetime_stop, s.datetime_stop)
JOIN good_seis_files AS gsf ON s.id=gsf.id
WHERE g.filename NOT IN (
	SELECT filename
	FROM links
	WHERE chain_id IN (
		SELECT DISTINCT chain_id
	    FROM links
		WHERE is_exist=0)
	);

CREATE VIEW minimal_energy
AS
SELECT time_intersection_id, minute_index, MIN(Ez) AS Ez
FROM seis_energy se
GROUP BY time_intersection_id;

CREATE VIEW energy_ratio
AS
SELECT se.time_intersection_id, se.minute_index, se.Ez/me.Ez AS Rz
FROM seis_energy se
JOIN minimal_energy me ON se.time_intersection_id=me.time_intersection_id;

CREATE VIEW grav_level
AS
SELECT se.time_intersection_id, ROUND(AVG(gmm.corr_grav), 4) AS quite_grav_level
FROM seis_energy AS se
JOIN median_energy AS me ON me.time_intersection_id=se.time_intersection_id
JOIN grav_seis_time_intersections gsti ON me.time_intersection_id=gsti.id
JOIN gravity_measures_minutes gmm ON gmm.grav_dat_file_id=gsti.grav_dat_id AND gmm.datetime_val=DATETIME(STRFTIME('%s', gsti.datetime_start)+(se.minute_index + 1) * 60, 'unixepoch')
WHERE se.Efull < me.Efull AND gmm.is_bad=0
GROUP BY se.time_intersection_id;

CREATE VIEW pre_correction
AS
SELECT gsti.id AS time_intersection_id, gmm.id AS grav_measure_id,
       gl.quite_grav_level, gmm.corr_grav, Rz
FROM seis_energy se
JOIN grav_seis_time_intersections gsti ON gsti.id=se.time_intersection_id
JOIN minimal_energy me ON me.time_intersection_id=se.time_intersection_id
JOIN gravity_measures_minutes gmm ON gmm.grav_dat_file_id =gsti.grav_dat_id AND gmm.datetime_val = DATETIME(STRFTIME('%s', gsti.datetime_start)+(se.minute_index + 1) * 60, 'unixepoch')
JOIN grav_level gl ON gl.time_intersection_id =se.time_intersection_id
JOIN energy_ratio er ON er.time_intersection_id=se.time_intersection_id AND er.minute_index=se.minute_index;

CREATE VIEW sensor_pairs
AS
SELECT l.chain_id, l.id AS link_id, gsti.id AS time_intersection_id,
       s.id AS seismometer_id, gdf.gravimeter_id
FROM grav_seis_time_intersections gsti
JOIN grav_dat_files gdf ON gdf.id=gsti.grav_dat_id
JOIN seis_files sf ON sf.id=gsti.seis_id
JOIN seismometers s ON s.id=sf.sensor_id
JOIN links l ON l.filename=gdf.filename;

CREATE VIEW post_correction
AS
SELECT l.chain_id, l.id AS link_id, gsti.id AS time_intersection_id,
       link_index,
       gmm.id-(SELECT MIN(id)
               FROM gravity_measures_minutes
               WHERE grav_dat_file_id=gdf.id) + 1 AS cycle_index,
       gmm.is_bad, ifnull(c.seis_corr, 0) AS seis_corr
FROM gravity_measures_minutes gmm
JOIN grav_seis_time_intersections gsti ON gmm.grav_dat_file_id=gsti.grav_dat_id
JOIN grav_dat_files gdf ON gdf.id =gmm.grav_dat_file_id
JOIN links l ON l.filename=gdf.filename
LEFT JOIN corrections c ON c.grav_measure_id=gmm.id
WHERE gsti.id IN (SELECT id FROM grav_seis_time_intersections);
