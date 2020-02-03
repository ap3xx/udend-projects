# DROP TABLES
ft_songplay_table_drop = "DROP TABLE IF EXISTS ft_songplays"
dim_user_table_drop = "DROP TABLE IF EXISTS dim_users"
dim_song_table_drop = "DROP TABLE IF EXISTS dim_songs"
dim_artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
dim_time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
ft_songplay_table_create = """
    CREATE TABLE ft_songplays(
        songplay_id SERIAL NOT NULL,
        start_time TIMESTAMP REFERENCES dim_time(start_time),
        user_id VARCHAR(50) REFERENCES dim_users(user_id),
        level VARCHAR(20),
        song_id VARCHAR(100) REFERENCES dim_songs(song_id),
        artist_id VARCHAR(100) REFERENCES dim_artists(artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR(255),
        user_agent TEXT,
        PRIMARY KEY (songplay_id)
    )"""

dim_user_table_create = """
    CREATE TABLE dim_users(
        user_id VARCHAR NOT NULL,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        gender VARCHAR(3),
        level VARCHAR(20),
        PRIMARY KEY (user_id)
    )"""

dim_song_table_create = """
    CREATE TABLE dim_songs(
        song_id VARCHAR(100) NOT NULL,
        title VARCHAR(255) NOT NULL,
        artist_id VARCHAR(100),
        year INTEGER,
        duration DOUBLE PRECISION,
        PRIMARY KEY (song_id)
    )"""

dim_artist_table_create = """
    CREATE TABLE dim_artists(
        artist_id VARCHAR(100) NOT NULL,
        name VARCHAR(255) NOT NULL,
        location VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        PRIMARY KEY (artist_id)
    )"""


dim_time_table_create = """
    CREATE TABLE dim_time(
        start_time TIMESTAMP,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER,
        PRIMARY KEY (start_time)
    )"""

# INSERT RECORDS
dim_user_table_insert = """
    INSERT INTO dim_users (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (user_id)
    DO UPDATE
    SET level = EXCLUDED.level"""

dim_song_table_insert = """
    INSERT INTO dim_songs 
    (song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING"""

dim_artist_table_insert = """
    INSERT INTO dim_artists 
    (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) 
    DO NOTHING"""

dim_time_table_insert = """
    INSERT INTO dim_time 
    (start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) 
    DO NOTHING"""

ft_songplay_table_insert = """
    INSERT INTO ft_songplays 
        (start_time, user_id, level, song_id, artist_id, session_id, 
        location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

# FIND SONGS BY ARTIST ID
dim_song_select = """
    SELECT s.song_id, a.artist_id FROM dim_songs s, dim_artists a
    WHERE s.artist_id = a.artist_id  
        AND s.title = %s
        AND a.name = %s
        AND s.duration = %s"""

# QUERY LISTS
create_table_queries = [dim_user_table_create, dim_song_table_create, dim_artist_table_create, dim_time_table_create,
                        ft_songplay_table_create]
drop_table_queries = [dim_user_table_drop, dim_song_table_drop, dim_artist_table_drop, dim_time_table_drop,
                      ft_songplay_table_drop]
