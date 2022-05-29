import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR
        , auth VARCHAR
        , firstName VARCHAR
        , gender VARCHAR
        , itemInSession INTEGER
        , lastName TEXT
        , length NUMERIC
        , level VARCHAR
        , location VARCHAR
        , method VARCHAR
        , page VARCHAR
        , registration VARCHAR
        , session_id VARCHAR
        , song VARCHAR
        , status INT
        , ts BIGINT
        , userAgent VARCHAR
        , user_id VARCHAR
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER NOT NULL
        , artist_id TEXT NOT NULL
        , artist_latitude DECIMAL
        , artist_longitude DECIMAL
        , artist_location VARCHAR
        , artist_name TEXT
        , song_id TEXT NOT NULL
        , title TEXT
        , duration NUMERIC
        , year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY
        , start_time TIMESTAMP
        , user_id VARCHAR NOT NULL
        , level TEXT
        , song_id VARCHAR NOT NULL
        , artist_id VARCHAR NOT NULL
        , session_id VARCHAR NOT NULL
        , location TEXT
        , user_agent TEXT
    )
    DISTKEY (song_id)
    SORTKEY(start_time, session_id);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY
        , first_name TEXT
        , last_name TEXT
        , gender TEXT
        , level TEXT
    )
        SORTKEY (user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY
        , title TEXT
        , artist_id VARCHAR NOT NULL
        , year INT
        , duration NUMERIC
    )
    SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY
        , name VARCHAR
        , location TEXT
        , latitude NUMERIC
        , longitude NUMERIC
    )
    SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY
        , hour INTEGER
        , day INTEGER
        , week INTEGER
        , month INTEGER
        , year INTEGER
        , weekday VARCHAR
    )
    SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = (
    f"""
        COPY staging_events 
        FROM {config['S3']['LOG_DATA']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        FORMAT JSON AS {config['S3']['LOG_JSONPATH']}
        REGION 'us-west-2'
    """
)
staging_songs_copy = (
    f"""
        COPY staging_songs
        FROM {config['S3']['song_data']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        FORMAT JSON AS 'auto'
        REGION 'us-west-2'
    """
)
 
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time
        , user_id
        , level
        , song_id
        , artist_id
        , session_id
        , location
        , user_agent
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second'
        , e.user_id AS user_id
        , e.level
        , s.song_id AS song_id
        , s.artist_id AS artist_id
        , e.session_id AS session_id
        , e.location
        , e.userAgent AS user_agent
    FROM staging_events e 
    INNER JOIN staging_songs s
        ON e.song = s.title 
          AND e.artist = s.artist_name
          AND s.duration > 0
    WHERE s.song_id IS NOT NULL
        AND e.session_id IS NOT NULL
        AND s.artist_id IS NOT NULL
        AND e.user_id IS NOT NULL
        AND e.page = 'NextSong'
    ;
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id
        , first_name
        , last_name
        , gender
        , level
    )
    SELECT DISTINCT
        user_id
        , firstName
        , lastName
        , gender
        , level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id
        , title
        , artist_id
        , year
        , duration
    )
    SELECT DISTINCT
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
        AND artist_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id
        , name
        , location
        , latitude
        , longitude
    )
    SELECT DISTINCT
        artist_id
        , artist_name AS name
        , artist_location AS location
        , artist_latitude AS latitude
        , artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time
        , hour
        , day
        , week
        , month
        , year
        , weekday
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time
        , EXTRACT(HOUR FROM start_time) AS hour
        , EXTRACT(DAY FROM start_time) AS day
        , EXTRACT(WEEKS FROM start_time) AS week
        , EXTRACT(MONTH FROM start_time) AS month
        , EXTRACT(YEAR FROM start_time)::INTEGER AS year
        , to_char(start_time, 'Day') AS weekday
    FROM staging_events;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
