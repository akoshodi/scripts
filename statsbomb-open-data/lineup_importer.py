import json
import psycopg2
import os

# Connect to the PostgreSQL database
conn = psycopg2.connect("dbname=statsbomb user=akoshodi password=R7yth3m3r#@\_ host=/var/run/postgresql")
cur = conn.cursor()

# Create the lineup table if it doesn't exist
cur.execute("""
CREATE TABLE IF NOT EXISTS lineup (
    lineup_id SERIAL PRIMARY KEY,
    team_id INTEGER,
    team_name TEXT,
    player_id INTEGER,
    player_name TEXT,
    player_nickname TEXT,
    jersey_number INTEGER,
    country_id INTEGER,
    country_name TEXT,
    position_id INTEGER,
    position TEXT,
    from_time TEXT,
    to_time TEXT,
    from_period INTEGER,
    to_period INTEGER,
    start_reason TEXT,
    end_reason TEXT
)
""")

# Set the path to the folder containing the JSON files
folder_path = 'open-data/data/lineups'

# Iterate over the JSON files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.json'):
        file_path = os.path.join(folder_path, filename)

        # Open the JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Iterate over the JSON data and insert it into the lineup table
        for team in data:
            team_id = team['team_id']
            team_name = team['team_name']
            for player in team['lineup']:
                player_id = player['player_id']
                player_name = player['player_name']
                player_nickname = player['player_nickname'] if 'player_nickname' in player else None
                jersey_number = player['jersey_number']

                # Check if the 'country' key exists
                if 'country' in player:
                    country_id = player['country']['id']
                    country_name = player['country']['name']
                else:
                    country_id = None
                    country_name = None

                for position in player['positions']:
                    position_id = position['position_id']
                    position_name = position['position']
                    from_time = position['from']
                    to_time = position['to']
                    from_period = position['from_period']
                    to_period = position['to_period'] if 'to_period' in position else None
                    start_reason = position['start_reason']
                    end_reason = position['end_reason'] if 'end_reason' in position else None

                    sql = """
                    INSERT INTO lineup (team_id, team_name, player_id, player_name, player_nickname, jersey_number, country_id, country_name, position_id, position, from_time, to_time, from_period, to_period, start_reason, end_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (team_id, team_name, player_id, player_name, player_nickname, jersey_number, country_id, country_name, position_id, position_name, from_time, to_time, from_period, to_period, start_reason, end_reason)
                    cur.execute(sql, values)

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()