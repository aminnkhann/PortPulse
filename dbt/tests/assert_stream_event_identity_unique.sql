SELECT kafka_topic, kafka_partition, kafka_offset
FROM {{ ref('stg_stream_eta_changes') }}
GROUP BY kafka_topic, kafka_partition, kafka_offset
HAVING COUNT(*) > 1
