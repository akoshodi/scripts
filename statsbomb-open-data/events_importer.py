import json
import psycopg2
import psycopg2.extras
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to the PostgreSQL database
conn = psycopg2.connect("dbname=statsbomb user=akoshodi password=R7yth3m3r#@\_ host=/var/run/postgresql")
cur = conn.cursor()

# Create a table to store the data
create_table_query = """
CREATE TABLE IF NOT EXISTS events (
    id VARCHAR(36) PRIMARY KEY,
    index INT,
    period INT,
    timestamp VARCHAR(12),
    minute INT,
    second INT,
    type_id INT,
    type_name VARCHAR(50),
    possession BOOLEAN,
    possession_team_id INT,
    possession_team_name VARCHAR(50),
    play_pattern_id INT,
    play_pattern_name VARCHAR(50),
    team_id INT,
    team_name VARCHAR(50),
    player_id INT,
    player_name VARCHAR(100),
    position_id INT,
    position_name VARCHAR(50),
    location FLOAT[],
    duration FLOAT,
    under_pressure BOOLEAN,
    formation VARCHAR(3),
    lineup JSON,
    related_events VARCHAR(36)[],
    pass JSON,
    carry JSON,
    ball_receipt JSON,
    duel JSON,
    jersey_number INT,
    tactics JSON
)
"""
cur.execute(create_table_query)

# Folder path containing the JSON files
folder_path = "open-data/data/events"

# Loop through all files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith(".json"):
        file_path = os.path.join(folder_path, file_name)

        # Open the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

            # Prepare the values for insertion
            values = []
            for event in data:
                possession_value = event.get("possession")
                logging.debug(f"Event ID: {event.get('id')}, Possession Value: {possession_value}")

                if possession_value is None:
                    possession_boolean = None
                else:
                    possession_boolean = bool(possession_value)

                values.append(
                    (
                        event.get("id"),
                        event.get("index"),
                        event.get("period"),
                        event.get("timestamp"),
                        event.get("minute"),
                        event.get("second"),
                        event.get("type", {}).get("id"),
                        event.get("type", {}).get("name"),
                        possession_boolean,
                        event.get("possession_team", {}).get("id"),
                        event.get("possession_team", {}).get("name"),
                        event.get("play_pattern", {}).get("id"),
                        event.get("play_pattern", {}).get("name"),
                        event.get("team", {}).get("id"),
                        event.get("team", {}).get("name"),
                        event.get("player", {}).get("id"),
                        event.get("player", {}).get("name"),
                        event.get("position", {}).get("id"),
                        event.get("position", {}).get("name"),
                        event.get("location"),
                        event.get("duration"),
                        event.get("under_pressure"),
                        str(event.get("tactics", {}).get("formation")) if event.get("tactics") else None,
                        json.dumps(event.get("tactics", {}).get("lineup")) if event.get("tactics") else None,
                        [str(related_event) for related_event in event.get("related_events", [])] if event.get("related_events") else [],
                        json.dumps(event.get("pass")) if event.get("pass") else None,
                        json.dumps(event.get("carry")) if event.get("carry") else None,
                        json.dumps(event.get("ball_receipt")) if event.get("ball_receipt") else None,
                        json.dumps(event.get("duel")) if event.get("duel") else None,
                        event.get("jersey_number") if event.get("jersey_number") else None,
                        json.dumps(event.get("tactics")) if event.get("tactics") else None
                    )
                )

            # Insert the values into the database
            insert_query = """
            INSERT INTO events (
                id, index, period, timestamp, minute, second, type_id, type_name,
                possession, possession_team_id, possession_team_name, play_pattern_id, play_pattern_name,
                team_id, team_name, player_id, player_name, position_id, position_name,
                location, duration, under_pressure, formation, lineup, related_events,
                pass, carry, ball_receipt, duel, jersey_number, tactics
            ) VALUES %s
            """
            psycopg2.extras.execute_values(cur, insert_query, values)

    conn.commit()

# Close the database connection
cur.close()
conn.close()