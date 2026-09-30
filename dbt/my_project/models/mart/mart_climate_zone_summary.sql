{{ config(
    materialized='table',
    schema='marts'
) }}

WITH fact AS (
    SELECT * FROM {{ ref('fact_weather_observations') }}
)

SELECT
    climate_zone,
    wilaya,
    city_name,
    DATE(observation_time_local) AS observation_date,
    COUNT(*) AS total_observations,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
    ROUND(MIN(temperature)::numeric, 2) AS min_temperature,
    ROUND(MAX(temperature)::numeric, 2) AS max_temperature,
    ROUND((MAX(temperature) - MIN(temperature))::numeric, 2) AS diurnal_temperature_range,
    ROUND(AVG(apparent_temperature)::numeric, 2) AS avg_apparent_temperature,
    ROUND(AVG(relative_humidity)::numeric, 2) AS avg_relative_humidity,
    ROUND(AVG(surface_pressure)::numeric, 2) AS avg_surface_pressure,
    ROUND(AVG(wind_speed)::numeric, 2) AS avg_wind_speed,
    ROUND(MAX(wind_speed)::numeric, 2) AS max_wind_speed,
    ROUND(MAX(uv_index)::numeric, 2) AS max_uv_index,
    ROUND(SUM(precipitation)::numeric, 2) AS total_precipitation,
    COUNT(CASE WHEN heat_stress_category IN ('Danger', 'Extreme Danger') THEN 1 END) AS high_heat_stress_hours
FROM fact
GROUP BY
    climate_zone,
    wilaya,
    city_name,
    DATE(observation_time_local)
ORDER BY
    climate_zone,
    city_name,
    observation_date
