WITH tmp_source AS (
        SELECT
            par_process_date
            , key_event_info_event_time AS checkpoint_time,
            , key_entity_id
            {% if FEATURE_AUDIT -%}
            , feature_values AS feature_map
            {%endif -%}
        FROM green.raw_c_e_mdlsrvc_mdl_executed
        WHERE par_region = '{{REGION}}'
          AND par_process_date BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
          AND key_checkpoint = 'CHECKOUT_CONFIRM' -- update checkpoint if needed
          AND key_entity_name = 'consumer' -- update entity_name if needed
          )