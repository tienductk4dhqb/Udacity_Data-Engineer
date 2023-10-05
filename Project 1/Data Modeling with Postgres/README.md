* Summary project
    This a project to apply  data modeling with Postgres and build an ETL pipeline using Python
* Project Description
    A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.
    Project builds an ETL pipeline (Extract, Transform, Load) to create the DB and tables, fetch data from JSON files, process the data, and insert the the data to DB. As technologies, Project-1 uses python, SQL, Postgresql DB.
* Datasets
    We will use two datasets: 
    Songs: data/song_data
    Logs: data/log_data
    Songs dataset
    Each file on the path data/song_data will contain the following JSON object:
    {
        "num_songs": 1,
        "artist_id": "AR7G5I41187FB4CE6C",
        "artist_latitude": null,
        "artist_longitude": null,
        "artist_location": "London, England",
        "artist_name": "Adam Ant",
        "song_id": "SONHOTT12A8C13493C",
        "title": "Something Girls",
        "duration": 233.40363,
        "year": 1982
    }
    Logs dataset
    Each file on the path data/log_data will contain a list of the following object:
    {
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\"",
    "userId": "39"
    }
    
* Table and schema in project
    1. songplays(
        songplay_id SERIAL PRIMARY KEY NOT NULL, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar NULL, 
        song_id varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        session_id int NOT NULL, 
        location varchar NULL, 
        user_agent varchar NULL
       );    

    2. users(
        user_id int PRIMARY KEY UNIQUE NOT NULL, 
        first_name varchar NULL, 
        last_name varchar NULL, 
        gender varchar NULL, 
        level varchar NULL
       );
        
    3. songs(
        song_id varchar PRIMARY KEY UNIQUE NOT NULL, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year int NOT NULL, 
        duration decimal NOT NULL
       );
    
    4. artist(
         artist_id varchar PRIMARY KEY UNIQUE NOT NULL, 
         name varchar NOT NULL, 
         location varchar NULL, 
         latitude float NULL, 
         longitude float NULL
        );
    


    5. time(
        start_time timestamp NOT NULL, 
        hour int NULL, 
        day int NULL, 
        week int NULL, 
        month int NULL, 
        year int NULL, 
        weekday int NULL
        );
    + Fact Table
    songplays: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    + Dimension Tables
    users: user info (columns: user_id, first_name, last_name, gender, level)
    songs: song info (columns: song_id, title, artist_id, year, duration)
    artists: artist info (columns: artist_id, name, location, latitude, longitude)
    time: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

* Proces
    + Database
    To create the database and tables we have a script: create_tables.py which will import all the queries stored in sql_queries and will execute them. First it will drop all existing tables and the database, then it will create the database and each table.

    Each table has an index (Primar Key) which will be assigned from the data obtained on the ETL process, on the case of the table time we are using the data type serial primary key to allow postgres auto increment the time_id value for every record inserted.

    We also are using the clause ON CONFLICT DO NOTHING on all the insert queries to avoid the script to raise an error if there is any conflict.
    + ETL
    The script etl.py provides all the functions to perform the ETL process.

* How to run the Python scripts
    + Create database and tables
        - Run create_tables.py to create the database and tables
            Type command line:
            python3 create_tables.py
            Output: Tables dropped successfully.
                    Tables created successfully.
        - Optional: Run test.ipynb Juppyter notebook to confirm the creation of the tables with the correct columns.
        - Run etl.py
            Type command line
            python3 etl.py

