    SELECT
         extract(epoch from event_info_event_time)::BIGINT event_time  --## event_time ##
         , key_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , {{ENTITY_COLUMN_NAME}} AS entity_id    --## entity_id ##
         {% for column_name in OTHER_COLUMN_NAMES -%}
            , {{column_name}}
         {% endfor -%}
--- always use event table as the main table
    FROM red.raw_r_e_payment_fraud_info
    WHERE par_region = '{{REGION}}'
      AND par_process_date BETWEEN '{{EVENT_START_DATE}}' AND '{{END_DATE}}'
      ORDER BY entity_id
