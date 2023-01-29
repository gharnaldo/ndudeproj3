import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songsplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(200),
    auth VARCHAR(10),
    firstname VARCHAR(10),
    gender VARCHAR(1),
    iteminsession INT,
    lastname VARCHAR(10),
    length NUMERIC, 
    level VARCHAR(5),
    location VARCHAR(50),
    method VARCHAR(5),
    page VARCHAR(20),
    registration FLOAT8,
    sessionid INT,
    song VARCHAR(200),
    status INT,
    ts BIGINT,
    useragent VARCHAR(150),
    userid INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(20),
    artist_latitude FLOAT,
    artist_longitude FLOAT, 
    artist_location VARCHAR(200),
    artist_name VARCHAR(200), 
    song_id VARCHAR(20), 
    title VARCHAR(200),
    duration FLOAT,
    year INT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songsplay (
    songplay_id INT IDENTITY PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(5),
    song_id VARCHAR(20),
    artist_id VARCHAR(20),
    session_id INT,
    location VARCHAR(50),
    user_agent VARCHAR(150));
""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY, 
    first_name VARCHAR(10),                             
    last_name VARCHAR(10),                            
    gender VARCHAR(1), 
    level VARCHAR(5));
""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    artist_id VARCHAR(20),
    year INT,
    duration FLOAT NOT NULL); 
""")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(200) NOT NULL, 
    location VARCHAR(200), 
    latitude FLOAT, 
    longitude FLOAT);
""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time timestamp PRIMARY KEY, 
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR(1));
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
    INSERT INTO songsplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
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
INSERT INTO users
    SELECT
        distinct e.userid,e.firstname,e.lastname,e.gender,e.level
    FROM 
        staging_events e
    WHERE
        e.page = 'NextSong'
""")
song_table_insert = ("""
INSERT INTO songs
    SELECT
        distinct s.song_id, s.title, s.artist_id, s.year, s.duration
    FROM 
        staging_songs s
""")
artist_table_insert = ("""
INSERT into artists
    SELECT
        distinct s.artist_id,s.artist_name,s.artist_location,
        s.artist_latitude,s.artist_longitude
    FROM 
        staging_songs s
""")
time_table_insert = ("""
INSERT into times
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
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
copy_table_queries = [staging_events_copy, staging_songs_copy]
