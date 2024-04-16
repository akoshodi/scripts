import json
import psycopg2

# Connect to the PostgreSQL database
# conn = psycopg2.connect("dbname=statsbomb user=akoshodi password=R7yth3m3r#@_")
conn = psycopg2.connect("dbname=statsbomb user=akoshodi password=R7yth3m3r#@_ host=/var/run/postgresql")
cur = conn.cursor()

# Create the competition table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS competition (
        competition_id INTEGER,
        season_id INTEGER,
        country_name TEXT,
        competition_name TEXT,
        competition_gender TEXT,
        competition_youth BOOLEAN,
        competition_international BOOLEAN,
        season_name TEXT,
        match_updated TIMESTAMP,
        match_updated_360 TIMESTAMP,
        match_available_360 TIMESTAMP,
        match_available TIMESTAMP,
        PRIMARY KEY (competition_id, season_id)
    )
""")

# Open the JSON file
with open('open-data/data/competitions.json', 'r') as f:
    data = json.load(f)

# Iterate over the JSON data and insert it into the competition table
for competition in data:
    competition_id = competition['competition_id']
    season_id = competition['season_id']
    country_name = competition['country_name']
    competition_name = competition['competition_name']
    competition_gender = competition['competition_gender']
    competition_youth = competition['competition_youth']
    competition_international = competition['competition_international']
    season_name = competition['season_name']
    match_updated = competition['match_updated']
    match_updated_360 = competition['match_updated_360']
    match_available_360 = competition['match_available_360']
    match_available = competition['match_available']


    sql = """
        INSERT INTO competition (competition_id, season_id, country_name, competition_name, competition_gender, competition_youth, competition_international, season_name, match_updated, match_updated_360, match_available_360, match_available)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (competition_id, season_id, country_name, competition_name, competition_gender, competition_youth, competition_international, season_name, match_updated, match_updated_360, match_available_360, match_available)

    cur.execute(sql, values)

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()