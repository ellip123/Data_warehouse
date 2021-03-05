import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTs songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
"""
This staging_events tables save data extracted from S3.
"""
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId varchar
);

""")

"""
This staging_songs tables save data extracted from S3.
"""

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id integer identity(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar DISTKEY,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar
    )diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id varchar PRIMARY KEY SORTKEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    ) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY SORTKEY,
    title varchar,
    artist_id varchar DISTKEY,
    year integer,
    duration float
    )diststyle key;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY SORTKEY,
    name varchar,
    location varchar,
    latitude float,
    longitude float
    )diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer DISTKEY,
    weekday varchar
    )diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
select DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
from staging_songs ss
join staging_events se
on ss.title = se.song and se.artist = ss.artist_name
and se.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong'
AND userId NOT IN (SELECT DISTINCT user_id FROM users)
;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
AND song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
insert into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
