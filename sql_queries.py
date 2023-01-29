import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(MAX),
    auth VARCHAR(MAX),
    FirstName VARCHAR(MAX),
    gender VARCHAR(MAX),
    itemInSession INT,
    lastName VARCHAR(MAX),
    length NUMERIC, 
    level VARCHAR(MAX),
    location VARCHAR(MAX),
    method VARCHAR(MAX),
    page VARCHAR(MAX),
    registration float8,
    sessionId INT,
    song VARCHAR(MAX),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(MAX),
    userId INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(MAX),
    artist_latitude FLOAT,
    artist_longitude FLOAT, 
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX), 
    song_id VARCHAR(MAX), 
    title VARCHAR(MAX),
    duration FLOAT,
    year INT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id INT IDENTITY PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar);
""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id int PRIMARY KEY, 
    first_name varchar,                             
    last_name varchar,                            
    gender varchar, 
    level varchar);
""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration float NOT NULL); 
""")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float);
""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    start_time timestamp PRIMARY KEY, 
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}'
region 'us-west-2' format as JSON {};
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2' format as JSON 'auto';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert =  ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        e.userid,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid,
        e.location,
        e.useragent
    FROM staging_events e
    LEFT JOIN staging_songs s ON
        e.song = s.title AND
        e.artist = s.artist_name AND
        ABS(e.length - s.duration) < 2
    WHERE
        e.page = 'NextSong'
""")
user_table_insert = ("""
INSERT INTO user_table
    (
        user_id,first_name, last_name,gender,level
    )
    SELECT
        distinct e.userid,e.firstname,e.lastname,e.gender,e.level
    FROM 
        staging_events e
    WHERE
        e.page = 'NextSong'
""")
song_table_insert = ("""
INSERT INTO song_table
    (
        song_id,title,artist_id,year,duration
    )
    SELECT
        distinct s.song_id, s.title, s.artist_id, s.year, s.duration
    FROM 
        staging_songs s
""")
artist_table_insert = ("""
INSERT into artist_table
    (
        artist_id, name, location, latitude,longitude
    )
    SELECT
        distinct s.artist_id,s.artist_name,s.artist_location,
        s.artist_latitude,s.artist_longitude
    FROM 
        staging_songs s
""")
time_table_insert = ("""
INSERT into time_table
    (
        start_time, hour, day, week, month, year, weekday
    )
    SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        extract(hour from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as hour,
        extract(day from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as day,
        extract(week from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as week,
        extract(month from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as month,
        extract(year from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as year,
        extract(weekday from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as weekday       
    FROM 
        staging_events e
    WHERE
        e.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
#create_table_queries = [staging_events_table_create, staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#drop_table_queries = [staging_events_table_drop, staging_songs_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
copy_table_queries = [staging_events_copy, staging_songs_copy]
