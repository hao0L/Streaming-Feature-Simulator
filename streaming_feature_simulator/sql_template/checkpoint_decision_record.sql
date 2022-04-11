WITH tmp_source AS (
    SELECT par_process_date
         , key_created_at AS checkpoint_time
         , key_consumer_id
         , key_order_token
         , key_merchant_id_main
         , key_device_fingerprint_hash
         , consumer_email
         , request
         , rules_variables AS feature_map
    FROM red.raw_c_e_fc_decision_record
    WHERE par_region = '{{REGION}}'
      AND par_process_date BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
      AND key_checkpoint = '{{CHECKPOINT}}'
)