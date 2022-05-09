    SELECT
         key_event_time::BIGINT AS event_time  --## event_time ##
         , key_entity_id AS consumer_uuid --## consumer_uuid ##
         , {{ENTITY_COLUMN_NAME}} AS entity_id    --## entity_id ##
         {% for column_name in OTHER_COLUMN_NAMES -%}
            , {{column_name}}
         {% endfor -%}
    FROM green.raw_c_e_mdlsrvc_mdl_executed
    WHERE par_region = '{{REGION}}'
      AND par_process_date BETWEEN '{{EVENT_START_DATE}}' AND '{{END_DATE}}'
      AND key_checkpoint = '{{CHECKPOINT}}' --#update checkpoint#
      AND key_entity_name = 'consumer' --#update entity_name if needed#
      ORDER BY entity_id


