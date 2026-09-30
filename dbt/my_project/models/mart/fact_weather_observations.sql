{{ config(
    materialized='table',
    schema='marts'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_weather_observations') }}
),

cities AS (
    SELECT * FROM {{ ref('dim_cities') }}
),

weather_codes AS (
    SELECT * FROM {{ ref('dim_weather_codes') }}
),

joined AS (
    SELECT
        s.raw_id,
        c.city_id,
        c.name AS city_name,
        c.wilaya,
        c.climate_zone,
        c.latitude,
        c.longitude,
        c.elevation_m,
        s.temperature,
        s.apparent_temperature,
        s.relative_humidity,
        s.surface_pressure,
        s.wind_speed,
        s.uv_index,
        s.precipitation,
        s.weather_code,
        COALESCE(w.weather_description, 'Unknown (' || s.weather_code || ')') AS weather_description,
        COALESCE(w.weather_category, 'Other') AS weather_category,
        s.observation_time_utc,
        s.observation_time_local,
        s.utc_offset_seconds,
        s.inserted_at
    FROM staged s
    INNER JOIN cities c
        ON LOWER(s.city_name) = LOWER(c.name)
    LEFT JOIN weather_codes w
        ON s.weather_code = w.weather_code
),

calculated_deltas AS (
    SELECT
        *,
        MD5(city_id || '_' || observation_time_local::text) AS observation_id,
        LAG(temperature, 1) OVER (
            PARTITION BY city_id 
            ORDER BY observation_time_local
        ) AS prev_hourly_temperature,
        ROUND((temperature - LAG(temperature, 1) OVER (
            PARTITION BY city_id 
            ORDER BY observation_time_local
        ))::numeric, 2) AS hourly_temperature_delta
    FROM joined
)

SELECT
    observation_id,
    city_id,
    city_name,
    wilaya,
    climate_zone,
    latitude,
    longitude,
    elevation_m,
    temperature,
    prev_hourly_temperature,
    hourly_temperature_delta,
    apparent_temperature,
    relative_humidity,
    surface_pressure,
    wind_speed,
    uv_index,
    precipitation,
    weather_code,
    weather_description,
    weather_category,
    CASE 
        WHEN apparent_temperature >= 41.0 OR (temperature >= 38.0 AND relative_humidity >= 40.0) THEN 'Extreme Danger'
        WHEN apparent_temperature >= 35.0 OR (temperature >= 33.0 AND relative_humidity >= 45.0) THEN 'Danger'
        WHEN apparent_temperature >= 29.0 OR (temperature >= 28.0 AND relative_humidity >= 50.0) THEN 'Extreme Caution'
        WHEN apparent_temperature >= 24.0 THEN 'Caution'
        WHEN apparent_temperature < 5.0 THEN 'Cold Stress'
        ELSE 'Comfortable'
    END AS heat_stress_category,
    observation_time_utc,
    observation_time_local,
    utc_offset_seconds,
    inserted_at
FROM calculated_deltas
