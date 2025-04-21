class SqlQueries:
    # Staging tables
    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender CHAR(1),
            itemInSession INTEGER,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId INTEGER,
            song VARCHAR,
            status INTEGER,
            ts BIGINT,
            userAgent VARCHAR,
            userId INTEGER
        )
    """
    
    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year INTEGER
        )
    """
    
    # Fact table
    songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INTEGER NOT NULL,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INTEGER,
            location VARCHAR,
            user_agent VARCHAR
        )
    """
    
    songplay_table_insert = """
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT 
            TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
            e.userId as user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent
        FROM staging_events e
        LEFT JOIN staging_songs s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            e.length = s.duration
        WHERE e.page = 'NextSong'
    """
    
    # Dimension tables
    user_table_create = """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender CHAR(1),
            level VARCHAR
        )
    """
    
    user_table_insert = """
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong' AND user_id IS NOT NULL
    """
    
    song_table_create = """
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            artist_id VARCHAR,
            year INTEGER,
            duration FLOAT
        )
    """
    
    song_table_insert = """
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """
    
    artist_table_create = """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        )
    """
    
    artist_table_insert = """
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """
    
    time_table_create = """
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INTEGER,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            weekday INTEGER
        )
    """
    
    time_table_insert = """
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT 
            start_time,
            EXTRACT(hour FROM start_time) as hour,
            EXTRACT(day FROM start_time) as day,
            EXTRACT(week FROM start_time) as week,
            EXTRACT(month FROM start_time) as month,
            EXTRACT(year FROM start_time) as year,
            EXTRACT(weekday FROM start_time) as weekday
        FROM songplays
    """
    
    # Data quality checks
    data_quality_check_queries = [
        {'test_sql': "SELECT COUNT(*) FROM users WHERE user_id IS NULL", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM songplays WHERE user_id IS NULL", 'expected_result': 0}
    ]
