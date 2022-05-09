    SELECT
         t1.key_event_info_event_time::BIGINT AS event_time  --## event_time ##
         , t1.consumer_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , t3.{{ENTITY_COLUMN_NAME}} AS entity_id    --## entity_id ##
         {% for column_name in OTHER_COLUMN_NAMES -%}
            , t1.{{column_name}}
         {% endfor -%}
--- always use event table as the main table
    FROM green.raw_c_e_order t1
             INNER JOIN green.raw_c_e_consumer_session_order_token t2
                        ON t1.par_region = '{{REGION}}'
                            AND t1.par_process_date BETWEEN '{{EVENT_START_DATE}}' AND '{{END_DATE}}'
                            AND t1.key_token = t2.token
                            AND ABS(t1.key_event_info_event_time - t2.key_created_at) <= 1 * 30 * 60 * 1000
             INNER JOIN green.raw_c_e_consumer_session t3
                        ON t3.par_region = '{{REGION}}'
                            AND t3.par_process_date BETWEEN '{{EVENT_START_DATE}}' AND '{{END_DATE}}'
                            AND t2.key_session_id = t3.key_session_id
                             AND ABS(t2.key_created_at - t3.created_at) <= 1 * 30 * 60 * 1000
    WHERE t1.par_region = '{{REGION}}'
      AND t1.par_process_date BETWEEN '{{EVENT_START_DATE}}' AND '{{END_DATE}}'
      ORDER BY entity_id