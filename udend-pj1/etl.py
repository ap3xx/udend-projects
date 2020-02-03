import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Load and process song files, 
    parsing song and artist information then
    inserting them into the DB.

    Args:
        cur: DB cursor.
        filepath: Path to file being processed.

    Returns:
        No return values.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    for i, row in song_df.iterrows():
        cur.execute(dim_song_table_insert, list(row))
    
    # insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    for i, row in artist_df.iterrows():
        cur.execute(dim_artist_table_insert, list(row))


def process_log_file(cur, filepath):
    """
    Load and process log files, 
    parsing timestamp and user information then
    inserting them into the DB.
    After that, saves the log information into
    the fact table, linking the relations.

    Args:
        cur: DB cursor.
        filepath: Path to file being processed.

    Returns:
        No return values.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    t = pd.DataFrame(df['ts'])
    
    # insert time data records
    time_data = (t['ts'], t['ts'].dt.hour, t['ts'].dt.day, t['ts'].dt.weekofyear, t['ts'].dt.month, t['ts'].dt.year, t['ts'].dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(dim_time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(dim_user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(dim_song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(ft_songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    List all the files within given path prefix
    then calls the given function to process those
    listed files one by one.

    Args:
        cur: DB cursor.
        conn: DB cursor.
        filepath: Path prefix at where files will be listed.
        func: function to process those files.

    Returns:
        No return values.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()