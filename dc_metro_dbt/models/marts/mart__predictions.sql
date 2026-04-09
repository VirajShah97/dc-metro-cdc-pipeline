WITH predictions AS (
    SELECT
        prediction_id,
        station_code,
        station_name,
        line_code,
        car_count,
        destination_code,
        destination_name,
        platform_group,
        minutes,
        CASE
            WHEN minutes IN ('ARR', 'BRD') THEN NULL
            ELSE TRY_CAST(minutes AS INT)
        END AS minutes_numeric,
        is_anomaly,
        anomaly_reasons,
        ingested_at,
        loaded_at,
        EXTRACT(HOUR FROM ingested_at) AS hour_of_day,
        DAYOFWEEK(ingested_at) AS day_of_week,
        CASE
            WHEN EXTRACT(HOUR FROM ingested_at) BETWEEN 6 AND 9 THEN 'Morning Rush'
            WHEN EXTRACT(HOUR FROM ingested_at) BETWEEN 16 AND 19 THEN 'Evening Rush'
            ELSE 'Off-Peak'
        END AS time_period
    FROM {{ ref('stg_predictions') }}
),

with_volatility AS (
    SELECT
        *,
        LAG(minutes_numeric) OVER (
            PARTITION BY station_code, line_code, destination_code, platform_group
            ORDER BY ingested_at
        ) AS prev_minutes_numeric
    FROM predictions
)

SELECT
    *,
    prev_minutes_numeric - minutes_numeric AS minutes_diff,
    CASE
        WHEN prev_minutes_numeric IS NULL THEN FALSE
        WHEN (prev_minutes_numeric - minutes_numeric) < 0 THEN TRUE
        ELSE FALSE
    END AS is_volatile
FROM with_volatility