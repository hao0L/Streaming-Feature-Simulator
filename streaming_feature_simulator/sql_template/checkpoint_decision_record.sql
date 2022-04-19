WITH tmp_source AS (
    SELECT par_process_date
         , key_created_at AS checkpoint_time
         , key_consumer_id
         , key_order_token
         , key_merchant_id_main
         , key_device_fingerprint_hash
         , consumer_email
         , request
         , (CASE
                    WHEN NOT is_valid_json(rules_variables)
                        THEN rtrim(regexp_substr(rules_variables, '^.*[,]'),',') || '}'
                    ELSE rules_variables END) AS feature_map
    FROM red.raw_c_e_fc_decision_record
    WHERE par_region = '{{REGION}}'
      AND par_process_date BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
      AND key_checkpoint = '{{CHECKPOINT}}'
)