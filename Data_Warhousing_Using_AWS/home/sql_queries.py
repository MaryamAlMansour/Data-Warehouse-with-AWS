import configparser
import json


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create = ("""
                                  CREATE TABLE IF NOT EXISTS staging_events(
                                        artist varchar,
                                        auth varchar,
                                        firstname varchar, 
                                        gender char(1), 
                                        iteminsession int,
                                        lastname varchar, 
                                        length float, 
                                        level varchar, 
                                        location varchar, 
                                        method varchar, 
                                        page varchar, 
                                        registration float, 
                                        sessionid int, 
                                        song text, 
                                        status int, 
                                        ts float, 
                                        useragent text, 
                                        userid int)
                                """) 

staging_songs_table_create = ("""
                                 CREATE TABLE IF NOT EXISTS staging_songs(
                                    num_songs int, 
                                    artist_id varchar, 
                                    artist_latitude DOUBLE PRECISION,
                                    artist_longitude DOUBLE PRECISION,
                                    artist_location varchar, 
                                    artist_name varchar, 
                                    song_id varchar, 
                                    title varchar, 
                                    duration float, 
                                    year int,
                                    PRIMARY KEY (song_id))

                            """) 

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id bigint identity(0,1) PRIMARY KEY, 
                                start_time timestamp ,
                                userid int , 
                                level varchar, 
                                song_id varchar, 
                                artist_id varchar,
                                sessionid int, 
                                location varchar, 
                                useragent varchar)
                        """)

user_table_create = ("""
                         CREATE TABLE IF NOT EXISTS users(
                             userid VARCHAR,
                             first_name VARCHAR(255),
                             last_name VARCHAR(255),
                             gender VARCHAR(1),
                             level VARCHAR(50),
                             PRIMARY KEY (userid))
                     """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS song(
                          song_id VARCHAR(100),
                          title VARCHAR(255),
                          artist_id VARCHAR(100) NOT NULL,
                          year INTEGER,
                          duration DOUBLE PRECISION,
                          PRIMARY KEY (song_id))
                     """)

artist_table_create = ("""
                           CREATE TABLE IF NOT EXISTS artist(
                              artist_id VARCHAR(100),
                              name VARCHAR(255),
                              location VARCHAR(255),
                              latitude DOUBLE PRECISION,
                              longtitude DOUBLE PRECISION,
                              PRIMARY KEY (artist_id))
                       """)

time_table_create = ("""
                         CREATE TABLE IF NOT EXISTS time(
                          start_time TIMESTAMP,
                          hour INTEGER,
                          day INTEGER,
                          week INTEGER,
                          month INTEGER,
                          year INTEGER,
                          weekday INTEGER,
                          PRIMARY KEY (start_time))
                          
                     """)

# STAGING TABLES ( COPY DATA FROM S3 TO STAGING TABLES)

staging_events_copy = "COPY staging_events \
from {} \
iam_role {} \
json {}".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = "COPY staging_songs \
from {} \
iam_role {} \
json 'auto' ".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# TABLE INSERTION

songplay_table_insert = ("""
                            INSERT INTO songplays(start_time, userid, level, song_id, artist_id, sessionid, location, useragent)
                            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                                se.userid AS user_id,
                                se.level AS level,
                                ss.song_id AS song_id,
                                ss.artist_id AS artist_id,
                                se.sessionid AS session_id,
                                se.location AS location,
                                se.useragent AS user_agent
                                FROM staging_events se  
                                LEFT JOIN staging_songs ss
                                    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
                                    WHERE page = 'NextSong'
                         """)
user_table_insert = ("""
                        INSERT INTO users(userid, first_name, last_name, gender, level)  
                        SELECT DISTINCT userid as user_id, firstname as first_name, lastname as last_name, gender, level
                        FROM staging_events
                        WHERE (staging_events.userid IS NOT NULL AND staging_events.level IS NOT NULL)
                     """)


song_table_insert = ("""
                        INSERT INTO song(song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT 
                            song_id, 
                            title,
                            artist_id,
                            year,
                            duration
                        FROM staging_songs
                        WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song)
                     """)


artist_table_insert = ("""
                           INSERT INTO artist(artist_id, name, location, latitude, longtitude) 
                           SELECT DISTINCT 
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude
                           FROM staging_songs
                           WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist)
                       """)

time_table_insert = ("""
                         INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                         SELECT 
                                start_time, 
                                EXTRACT(hr from start_time) AS hour,
                                EXTRACT(d from start_time) AS day,
                                EXTRACT(w from start_time) AS week,
                                EXTRACT(mon from start_time) AS month,
                                EXTRACT(yr from start_time) AS year, 
                                EXTRACT(weekday from start_time) AS weekday 
                         FROM (
                                SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time 
                                FROM staging_events s     
                              )
                         WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
                     """)
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
