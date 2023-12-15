import requests
import sqlite3
from geopy.geocoders import Nominatim

api_url = 'https://datausa.io/api/data'

# Set the parameters for the API call
parameters = {
    "drilldowns": "State",
    "measures": "Population",
    "year": "latest"
}

# Make the API request
response = requests.get(api_url, params=parameters)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    try:
        # Parse the JSON response
        data = response.json()

        # Connect to SQLite database
        conn = sqlite3.connect('datausa.db')
        cursor = conn.cursor()

        # Create a table (assuming specific columns)
        cursor.execute('''CREATE TABLE IF NOT EXISTS population_data (
                            geo_id TEXT,
                            geo_name TEXT,
                            year INTEGER,
                            population INTEGER,
                            latitude REAL,
                            longitude REAL,
                            PRIMARY KEY (geo_id)
                        );''')

        # Insert data into the table
        for item in data.get('data', []):
            try:
                geo_id = item.get('ID State')
                geo_name = item.get('State')
                year = item.get('ID Year')
                population = item.get('Population')

                # Geocode the state to get latitude and longitude
                location_name = f"{geo_name}, USA"
                loc = Nominatim(user_agent="GetLoc")
                get_loc = loc.geocode(location_name)

                if get_loc:
                    latitude = get_loc.latitude
                    longitude = get_loc.longitude

                    cursor.execute("INSERT OR IGNORE INTO population_data VALUES (?, ?, ?, ?, ?, ?);",
                                   (geo_id, geo_name, year, population, latitude, longitude))
                else:
                    print(f"Geocode not found for {location_name} - Skipping this item.")

            except KeyError as e:
                print(f"KeyError: {e} - Skipping this item.")

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

        print("Data loaded into SQLite database.")
    except requests.exceptions.JSONDecodeError:
        print("Error decoding JSON. Response content:")
        print(response.content.decode('utf-8'))
else:
    # Print an error message if the request was not successful
    print(f"Error: {response.status_code}")
    print(response.content.decode('utf-8'))




