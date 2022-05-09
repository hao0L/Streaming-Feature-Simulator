WITH tmp_source AS (
    SELECT par_process_date
         , key_event_info_event_time::BIGINT AS checkpoint_time
         , key_consumer_uuid
         , key_order_token
         , feature_map
    FROM red.raw_r_e_rekarma_rl_exec_rslt
    WHERE par_region = '{{REGION}}'
      AND par_process_date BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
      AND key_checkpoint = '{{CHECKPOINT}}'
)