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
        , sessioId VARCHAR
        , song VARCHAR
        , status INT
        , ts BIGINT
        , userAgent VARCHAR
        , userId VARCHAR
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER
        , artistId VARCHAR
        , artist_latitude VARCHAR
        , artist_longitude VARCHAR
        , artist_location VARCHAR
        , artist_name TEXT
        , songId VARCHAR
        , title TEXT
        , duration NUMERIC
        , year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER NOT NULL IDENTITY(0,1)
        , start_time TIMESTAMP NOT NULL
        , user_id VARCHAR NOT NULL
        , level TEXT NOT NULL
        , song_id VARCHAR NOT NULL
        , artist_id VARCHAR NOT NULL
        , session_id VARCHAR NOT NULL
        , location TEXT NOT NULL
        , user_agent TEXT NOT NULL
    )
    DISTKEY (song_id)
    SORTKEY(start_time, session_id)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY
        , first_name TEXT
        , last_name TEXT
        , artist_id VARCHAR NOT NULL
        , gender TEXT 
        , level TEXT
    )
    DISTSTYLE AUTO
    SORTKEY (user_id);

""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY
        , title TEXT
        , artist_id VARCHAR NOT NULL
        , year INT
        , duration NUMERIC NOT NULL
    )
    DISTSTYLE AUTO
    SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR NOT NULL PRIMARY KEY
        , name VARCHAR
        , location TEXT
        , latitude NUMERIC
        , longitude NUMERIC
    )
    DISTSTYLE AUTO
    SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY
        , hour INTEGER NOT NULL
        , day INTEGER NOT NULL
        , week INTEGER NOT NULL
        , month INTEGER NOT NULL
        , year INTEGER NOT NULL
        , weekday VARCHAR NOT NULL
    )
    DISTSTYLE AUTO
    SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = (
    f"""
        COPY staging_events 
        FROM {config['S3']['song_data']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        FORMAT JSON AS {config['S3']['LOG_JSONPATH']}
    """
)
staging_songs_copy = (
    f"""
        COPY staging_events 
        FROM {config['S3']['song_data']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        FORMAT JSON AS 'auto'
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
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second'
        , e.userId AS user_id
        , e.level
        , s.song_id
        , s.artist_id
        , e.sessionId AS session_id
        , e.location
        , e.userAgent as user_agent
    FROM staging_events e 
    INNER JOIN staging_songs s
        ON e.song = s.title AND e.artist = s.artist_name;
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT
        userId
        , firstName
        , lastName
        , gender
        , level
    FROM staging_events
    WHERE userId IS NOT NULL
        AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT
        artist_id
        , artist_location
        , artist_latitude
        , artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time
        , EXTRACT(HOUR FROM start_time) as hour
        , EXTRACT(DAY FROM start_time) as day
        , EXTRACT(WEEKS FROM start_time) as week
        , EXTRACT(MONTH FROM start_time) as month
        , EXTRACT(YEAR FROM start_time) as year
        , to_char(start_time, 'Day') as weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
