import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
print("Reloading Queries")

 
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS Events"
staging_songs_table_drop = "DROP TABLE IF EXISTS Songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist" 
time_table_drop = "DROP TABLE IF EXISTS dimTime"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE Events
(
  artist            varchar(256),
  auth              varchar(45),
  firstName         varchar(50),
  gender            varchar(1),
  itemInSession     smallint ,
  lastName          varchar(50),
  length            float,
  level             varchar(10),
  location          varchar(256),
  method            varchar(10),
  page              varchar(50),
  registration      float,
  sessionId         varchar(256), 
  song              varchar(256), 
  status            smallint, 
  ts                bigint,
  userAgent         varchar(1024),
  userId            int
)
""")

staging_songs_table_create = ("""
CREATE TABLE Songs
(
  num_songs bigint, 
  artist_id varchar(20), 
  artist_latitude float, 
  artist_longitude float, 
  artist_location varchar(256),  
  artist_name  varchar(1024), 
  song_id varchar(20),
  title varchar(256),  
  duration float, 
  year int
)
""")

songplay_table_create = ("""
CREATE TABLE songplay
(
  spid        int NOT NULL IDENTITY(0,1), 
  start_time  timestamp NOT NULL, 
  userid      int NOT NULL, 
  level       varchar(10) NOT NULL,  
  song_id     varchar(20) NOT NULL,  
  artist_id   varchar(20) NOT NULL,  
  session_id  varchar(256),  
  location    varchar(256),  
  user_agent  varchar(1024)
)
""")

user_table_create = ("""
CREATE TABLE dimUser
(
  uid         int NOT NULL IDENTITY(0,1), 
  userid      int NOT NULL,
  first_name  varchar(50) NOT NULL,
  last_name   varchar(50) NOT NULL,
  gender      varchar(1) NOT NULL, 
  level       varchar(10) NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE dimSong
(
  sid       int NOT NULL IDENTITY(0,1),
  song_id   varchar(20) NOT NULL, 
  title     varchar(256) NOT NULL,
  artist_id varchar(20) NOT NULL,
  year      int NOT NULL,
  duration  float NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE dimArtist
(
  aid         int  NOT NULL IDENTITY(0,1),
  artist_id   varchar(20) NOT NULL,
  name        varchar(1024) NOT NULL,
  location    varchar(256),
  lattitude   float,
  longitude   float
)
""")

time_table_create = ("""
CREATE TABLE dimTime
(
  tid         int NOT NULL IDENTITY(0,1), 
  start_time  timestamp NOT NULL,
  hour        smallint NOT NULL,
  dow         varchar(10) NOT NULL,
  day         smallint NOT NULL,
  week        smallint NOT NULL,
  month       smallint NOT NULL,
  year        smallint NOT NULL,
  weekday     boolean NOT NULL
)
""")


# STAGING TABLES

staging_events_copy = ("""
copy Events from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 's3://udacity-dend/log_json_path.json';
""").format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""
copy Songs from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(config.get("IAM_ROLE", "ARN"))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, userid, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT(timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') AS start_time,
        u.uid AS userid,
        e.level AS level,
        s.sid AS song_id,
        a.aid AS artist_id,
        e.sessionid AS session_id,
        a.location AS location,
        e.userAgent AS user_agent
FROM events e
JOIN dimUser u on (u.userid = e.userid)
JOIN dimArtist a on (a.name = e.artist)
JOIN dimSong s on (s.title = e.song AND s.artist_id = a.artist_id)
JOIN dimTime t on (t.start_time = start_time)
""")

user_table_insert = ("""
INSERT INTO dimUser (userid, first_name, last_name, gender, level)
SELECT  DISTINCT(e.userid) AS userid,
        e.firstname AS first_name,
        e.lastname AS last_name,
        e.gender AS gender,
        e.level AS level
FROM events e
WHERE e.userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dimSong (song_id, title, artist_id, year, duration)
SELECT  DISTINCT(s.song_id) AS song_id,
        s.title AS title,
        s.artist_id AS artist_id,
        s.year AS year,
        s.duration AS duration
FROM songs s;
""")

artist_table_insert = ("""
INSERT INTO dimArtist (artist_id, name, location, lattitude, longitude)
SELECT  DISTINCT(s.artist_id) AS artist_id,
        s.artist_name AS name,
        s.artist_location AS location,
        CAST(s.artist_latitude as float) AS lattitude,
        CAST(s.artist_longitude as float) AS longitude
FROM songs s;
""")

time_table_insert = ("""
INSERT INTO DIMTIME (start_time, hour, dow, day, week, month, year, weekday)
SELECT DISTINCT(timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') AS start_time,
           EXTRACT(hour FROM start_time)                              AS hour,
           to_char(start_time, 'day')                               AS dow,
           EXTRACT(day FROM start_time)                               AS day,
           EXTRACT(week FROM start_time)                              AS week,
           EXTRACT(month FROM start_time)                             AS month,
           EXTRACT(year FROM start_time)                              AS year,
           CASE WHEN EXTRACT(DOW FROM start_time) IN (0, 6) THEN false ELSE true END AS weekday
FROM events e;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
