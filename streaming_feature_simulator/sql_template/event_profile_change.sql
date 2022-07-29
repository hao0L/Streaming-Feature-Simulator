    SELECT
         EXTRACT('epoch' FROM event_info_event_time)::BIGINT *1000 AS event_time  --## event_time ##
         , consumer_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , {{ENTITY_COLUMN_NAME}} AS entity_id    --## entity_id ##
         {% for column_name in OTHER_COLUMN_NAMES -%}
            , {{column_name}}
         {% endfor -%}
--- always use event table as the main table
    FROM red.raw_c_e_consumer_profile_change
    WHERE par_region in ({{REGION}})
      AND par_process_date BETWEEN '{{EVENT_START_DATE}}' AND '{{END_DATE}}'
    ORDER BY entity_id