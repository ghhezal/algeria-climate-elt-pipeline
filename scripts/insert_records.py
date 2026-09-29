import os
from urllib.parse import urlencode
from datetime import datetime
import psycopg2
from api_request import fetch_data

# 6 Algerian Cities across distinct climate zones
# (city_name, latitude, longitude, climate_zone)
ALGERIAN_CITIES = (
    ("Biskra", 34.8504, 5.7281, "Saharan"),
    ("Algiers", 36.7538, 3.0588, "Mediterranean Coastal"),
    ("Oran", 35.6987, -0.6349, "Western Coastal"),
    ("Constantine", 36.3650, 6.6147, "High Plateaus"),
    ("Tamanrasset", 22.7850, 5.5228, "Deep Sahara/Mountain"),
    ("Annaba", 36.9000, 7.7667, "Eastern Coastal"),
)

METRICS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "surface_pressure",
    "wind_speed_10m",
    "uv_index",
    "precipitation",
    "weather_code",
]

def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "db"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "db"),
            user=os.getenv("POSTGRES_USER", "db_user"),
            password=os.getenv("POSTGRES_PASSWORD", "db_password"),
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_table(conn):
    print("Ensuring raw schema and raw.weather_observations table exist...")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE SCHEMA IF NOT EXISTS staging;
                CREATE SCHEMA IF NOT EXISTS marts;

                CREATE TABLE IF NOT EXISTS raw.weather_observations (
                    id SERIAL PRIMARY KEY,
                    city_name VARCHAR(64) NOT NULL,
                    latitude NUMERIC(7,4) NOT NULL,
                    longitude NUMERIC(7,4) NOT NULL,
                    temperature NUMERIC(5,2),
                    apparent_temperature NUMERIC(5,2),
                    relative_humidity NUMERIC(5,2),
                    surface_pressure NUMERIC(6,2),
                    wind_speed NUMERIC(5,2),
                    uv_index NUMERIC(4,2),
                    precipitation NUMERIC(5,2),
                    weather_code INTEGER,
                    observation_time TIMESTAMP NOT NULL,
                    utc_offset_seconds INTEGER,
                    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_raw_weather_city_time 
                ON raw.weather_observations(city_name, observation_time);
            """)
            conn.commit()
            print("Database schemas and raw table verified successfully.")
    except Exception as e:
        print(f"Error initializing database schemas/table: {e}")
        raise

def insert_current_observation(conn, city_name, lat, lon):
    query_params = {
        "latitude": lat,
        "longitude": lon,
        "current": ",".join(METRICS),
        "timezone": "auto",
    }
    url = f"https://api.open-meteo.com/v1/forecast?{urlencode(query_params)}"
    data = fetch_data(url)

    current = data.get("current", {})
    obs_time = datetime.fromisoformat(current["time"])
    utc_offset = data.get("utc_offset_seconds", 3600)

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.weather_observations (
                city_name, latitude, longitude,
                temperature, apparent_temperature, relative_humidity,
                surface_pressure, wind_speed, uv_index, precipitation,
                weather_code, observation_time, utc_offset_seconds, inserted_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.weather_observations
                WHERE city_name = %s AND observation_time = %s
            );
        """, (
            city_name, lat, lon,
            current.get("temperature_2m"),
            current.get("apparent_temperature"),
            current.get("relative_humidity_2m"),
            current.get("surface_pressure"),
            current.get("wind_speed_10m"),
            current.get("uv_index"),
            current.get("precipitation"),
            current.get("weather_code"),
            obs_time,
            utc_offset,
            city_name,
            obs_time,
        ))
    conn.commit()
    print(f"Ingested current observation for {city_name} ({obs_time})")

def main():
    conn = connect_to_db()
    try:
        create_table(conn)
        for city_name, lat, lon, _ in ALGERIAN_CITIES:
            insert_current_observation(conn, city_name, lat, lon)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
