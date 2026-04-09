WITH source AS (
    SELECT
        raw_data,
        loaded_at
    FROM {{ source('raw', 'predictions') }}
),

parsed AS (
    SELECT
        raw_data:id::INT                        AS prediction_id,
        raw_data:station_code::VARCHAR           AS station_code,
        raw_data:station_name::VARCHAR           AS station_name,
        raw_data:line_code::VARCHAR              AS line_code,
        raw_data:car_count::VARCHAR              AS car_count,
        raw_data:destination_code::VARCHAR       AS destination_code,
        raw_data:destination_name::VARCHAR       AS destination_name,
        raw_data:platform_group::VARCHAR         AS platform_group,
        raw_data:minutes::VARCHAR                AS minutes,
        raw_data:is_anomaly::BOOLEAN             AS is_anomaly,
        raw_data:anomaly_reasons::ARRAY          AS anomaly_reasons,
        TO_TIMESTAMP(raw_data:ingested_at::NUMBER / 1000000) AS ingested_at,
        loaded_at
    FROM source
)

SELECT * FROM parsed
WHERE raw_data:line_code::VARCHAR NOT IN ('--', 'No')