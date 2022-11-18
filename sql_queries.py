import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist          VARCHAR(MAX),
auth            VARCHAR(MAX), 
firstName       VARCHAR(MAX),
gender          VARCHAR(MAX),   
itemInSession   INTEGER,
lastName        VARCHAR(MAX),
length          FLOAT,
level           VARCHAR(MAX), 
location        VARCHAR(MAX),
method          VARCHAR(MAX),
page            VARCHAR(MAX),
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR(MAX),
status          INTEGER,
ts              BIGINT,
userAgent       VARCHAR(MAX),
userId          INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level VARCHAR(MAX), 
song_id VARCHAR(MAX), 
artist_id VARCHAR(MAX), 
session_id VARCHAR(MAX), 
location VARCHAR(MAX), 
user_agent VARCHAR(MAX)); 
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id INT PRIMARY KEY, 
first_name text NOT NULL, 
last_name text NOT NULL, 
gender varchar, 
level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
song_id VARCHAR(MAX) PRIMARY KEY, 
title text NOT NULL, 
artist_id text NOT NULL, 
year int, 
duration float NOT NULL); 
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
artist_id VARCHAR(MAX)  PRIMARY KEY, 
name text NOT NULL, 
location text, 
latitude float, 
longitude float); 
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY , 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int);
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS JSON 'auto' 
""").format( SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
start_time, 
user_id, 
level, 
song_id, 
artist_id, 
session_id, 
location, 
user_agent)
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title
AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, firstname, lastname, gender, level)
SELECT DISTINCT
user_id, 
first_name, 
last_name, 
gender, 
level
FROM staging_events
WHERE user_id IS NOT NULL page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id, 
title, 
artist_id, 
year, 
duration)
FROM starting_songs
where song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
artist_id, 
name, 
location, 
latitude, 
longitude)
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
start_time, 
hour, 
day, 
week, 
month, 
year, 
weekday)
FROM songplay;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
