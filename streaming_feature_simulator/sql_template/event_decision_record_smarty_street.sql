     WITH tmp_source AS (
        SELECT *
             , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(entity_id, 0),
                                      'delivery_line_1', TRUE) delivery_line_1
             , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(entity_id, 0),
                                      'delivery_line_2', TRUE) delivery_line_2
             , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(entity_id, 0),
                                      'last_line', TRUE) last_line
             , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(entity_id, 0),
                                      'address1', TRUE) address1
             , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(entity_id, 0),
                                      'address2', TRUE) address2
             , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(entity_id, 0),
                                      'address3', TRUE) address3
        FROM sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}}
        WHERE IS_VALID_JSON_ARRAY(entity_id)
    )
    SELECT par_region
         , par_process_date
         , checkpoint_time::BIGINT AS event_time
         , consumer_uuid
         , is_rejected
         , merchant_order_amount
         , (CASE
                WHEN par_region = 'US' THEN MD5(LOWER(TRIM(delivery_line_1) || ' ' || TRIM(delivery_line_2) || ' ' ||
                                                      TRIM(last_line)))
                ELSE MD5(LOWER(TRIM(address1) || ' ' || TRIM(address2) || ' ' || TRIM(address3))) END) entity_id
         , (CASE
                WHEN par_region = 'US' THEN LOWER(TRIM(delivery_line_1) || ' ' || TRIM(delivery_line_2) || ' ' ||
                                                  TRIM(last_line))
                ELSE LOWER(TRIM(address1) || ' ' || TRIM(address2) || ' ' || TRIM(address3)) END) entity_id_raw
    FROM tmp_source