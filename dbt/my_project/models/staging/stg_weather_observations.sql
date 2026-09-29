{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT
        id,
        city_name,
        latitude,
        longitude,
        temperature,
        apparent_temperature,
        relative_humidity,
        surface_pressure,
        wind_speed,
        uv_index,
        precipitation,
        weather_code,
        observation_time,
        utc_offset_seconds,
        inserted_at
    FROM {{ source('raw', 'weather_observations') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city_name, observation_time 
            ORDER BY inserted_at DESC, id DESC
        ) AS row_num
    FROM source_data
)

SELECT
    id AS raw_id,
    TRIM(city_name) AS city_name,
    latitude::NUMERIC(7,4) AS latitude,
    longitude::NUMERIC(7,4) AS longitude,
    temperature::NUMERIC(5,2) AS temperature,
    apparent_temperature::NUMERIC(5,2) AS apparent_temperature,
    relative_humidity::NUMERIC(5,2) AS relative_humidity,
    surface_pressure::NUMERIC(6,2) AS surface_pressure,
    wind_speed::NUMERIC(5,2) AS wind_speed,
    uv_index::NUMERIC(4,2) AS uv_index,
    precipitation::NUMERIC(5,2) AS precipitation,
    weather_code::INTEGER AS weather_code,
    observation_time AS observation_time_utc,
    (observation_time + (COALESCE(utc_offset_seconds, 3600) || ' seconds')::interval) AS observation_time_local,
    COALESCE(utc_offset_seconds, 3600) AS utc_offset_seconds,
    inserted_at
FROM deduplicated
WHERE row_num = 1
