"""Seed historical hourly weather observations for the 6 Algerian cities."""

from datetime import datetime
from urllib.parse import urlencode
from api_request import fetch_data
from insert_records import ALGERIAN_CITIES, METRICS, connect_to_db, create_table

def seed_city_history(conn, city_name, lat, lon):
    print(f"Seeding historical observations for {city_name}...")
    query_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(METRICS),
        "current": ",".join(METRICS),
        "past_days": 2,
        "forecast_days": 1,
        "timezone": "auto",
    }
    url = f"https://api.open-meteo.com/v1/forecast?{urlencode(query_params)}"
    data = fetch_data(url)

    hourly = data.get("hourly", {})
    utc_offset = data.get("utc_offset_seconds", 3600)
    current_time = datetime.fromisoformat(data["current"]["time"])

    rows = []
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    app_temps = hourly.get("apparent_temperature", [])
    humidities = hourly.get("relative_humidity_2m", [])
    pressures = hourly.get("surface_pressure", [])
    winds = hourly.get("wind_speed_10m", [])
    uvs = hourly.get("uv_index", [])
    precips = hourly.get("precipitation", [])
    codes = hourly.get("weather_code", [])

    for i in range(len(times)):
        obs_dt = datetime.fromisoformat(times[i])
        # Only seed up to the current observation time
        if obs_dt <= current_time:
            rows.append((
                city_name,
                lat,
                lon,
                temps[i] if i < len(temps) else None,
                app_temps[i] if i < len(app_temps) else None,
                humidities[i] if i < len(humidities) else None,
                pressures[i] if i < len(pressures) else None,
                winds[i] if i < len(winds) else None,
                uvs[i] if i < len(uvs) else None,
                precips[i] if i < len(precips) else None,
                codes[i] if i < len(codes) else None,
                obs_dt,
                utc_offset,
                city_name,
                obs_dt,
            ))

    if rows:
        with conn.cursor() as cur:
            cur.executemany("""
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
            """, rows)
        conn.commit()
    print(f"Successfully seeded {len(rows)} hourly observations for {city_name}.")

def main():
    conn = connect_to_db()
    try:
        create_table(conn)
        for city_name, lat, lon, _ in ALGERIAN_CITIES:
            seed_city_history(conn, city_name, lat, lon)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
