import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS app_user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_event (
artist TEXT,
auth TEXT,
firstName TEXT,
gender TEXT,
iteminsession INTEGER,
lastname TEXT,
length FLOAT,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration FLOAT,
sessionid INTEGER,
song TEXT,
status INTEGER,
ts TIMESTAMP,
useragent TEXT,
userid INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_song (
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR, 
artist_name VARCHAR, 
song_id VARCHAR, 
title VARCHAR, 
duration FLOAT, 
year INTEGER

);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay (
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey,
user_id INTEGER NOT NULL REFERENCES app_user(user_id),
level VARCHAR NOT NULL,
song_id VARCHAR NOT NULL REFERENCES song(song_id) distkey,
artist_id VARCHAR NOT NULL REFERENCES artist(artist_id),
session_id INTEGER NOT NULL,
location VARCHAR NOT NULL,
user_agent VARCHAR NOT NULL
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS app_user (
user_id INTEGER NOT NULL PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR NOT NULL,
level VARCHAR NOT NULL
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS song (
song_id VARCHAR PRIMARY KEY,
title VARCHAR(1024) NOT NULL,
artist_id VARCHAR NOT NULL REFERENCES artist(artist_id) sortkey distkey,
year INTEGER,
duration FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist (
artist_id VARCHAR PRIMARY KEY,
name VARCHAR(1024) NOT NULL,
location VARCHAR(1024),
latitude FLOAT,
longitude FLOAT
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
hour INTEGER, 
day INTEGER, 
week INTEGER, 
month INTEGER,
year INTEGER, 
weekday INTEGER
)
"""

# STAGING TABLES
staging_events_copy = """
COPY staging_event
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
timeformat 'epochmillisecs'
JSON '{}';
""".format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = """
COPY staging_song
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
JSON 'auto';
""".format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplay(
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent) 
SELECT DISTINCT
    se.ts AS start_time, 
    se.userId AS user_id, 
    se.level, 
    ss.song_id, 
    ss.artist_id, 
    se.sessionId AS session_id, 
    se.location, 
    se.userAgent AS user_agent
FROM staging_event se, staging_song ss
WHERE se.page = 'NextSong'
    AND se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
;
"""

user_table_insert = """
INSERT INTO app_user
SELECT user_id::INTEGER,
       first_name,
       last_name,
       gender,
       level
FROM (SELECT userid    AS user_id,
             firstname AS first_name,
             lastname  AS last_name,
             gender,
             level
      FROM staging_event
      WHERE user_id IS NOT NULL) AS temp
GROUP BY user_id, first_name, last_name, gender, level
ORDER BY user_id
;
"""

song_table_insert = """
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
  FROM staging_song;
"""

artist_table_insert = """
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_song;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    se.ts AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dayofweek FROM start_time) AS weekday
FROM (
    SELECT ts 
    FROM staging_event
    GROUP BY ts
    ) AS se
ORDER BY se.ts ASC

"""

# QUERY LISTS
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    time_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
