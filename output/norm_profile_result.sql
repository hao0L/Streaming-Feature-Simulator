----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_norm_profile;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_norm_profile DISTKEY(entity_id) AS (
WITH tmp_source AS (
    SELECT par_region
         , par_process_date
         , key_created_at::BIGINT AS checkpoint_time
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
    WHERE par_region = 'US'
      AND par_process_date BETWEEN '2022-05-14' AND '2022-05-17'
      AND key_checkpoint = 'CHECKOUT_CONFIRM'
)
    SELECT
           par_process_date
           , checkpoint_time
           , key_consumer_id AS consumer_uuid
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'SMARTY_STREET_PROFILE_ADDRESS_VERIFICATION',TRUE) AS entity_id
----- adding feature extractions for feature audit use case ----
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0', TRUE) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0', TRUE) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0', TRUE) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0', TRUE) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0', TRUE) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0', TRUE) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0
           FROM tmp_source
    WHERE IS_VALID_JSON(feature_map)
    ORDER BY entity_id
    )
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_norm_profile;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_norm_profile DISTKEY(entity_id) AS (
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
        FROM sandbox_analytics_us.tmp_feature_audit_feature_value_norm_address
        WHERE IS_VALID_JSON_ARRAY(entity_id) > 0
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
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_norm_profile;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_norm_profile DISTKEY(entity_id) AS (

    SELECT t2.entity_id
        , t2.checkpoint_time
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) THEN t1.consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*60*60*1000) THEN t1.consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) THEN t1.consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) THEN t1.consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) THEN t1.consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) THEN t1.consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0
        , COALESCE( ( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (t1.is_rejected = 'false') THEN t1.consumer_uuid ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0
        , COALESCE( ( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*60*60*1000) AND (t1.is_rejected = 'false') THEN t1.consumer_uuid ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0
        , COALESCE( ( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (t1.is_rejected = 'false') THEN t1.consumer_uuid ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0
        , COALESCE( ( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (t1.is_rejected = 'false') THEN t1.consumer_uuid ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0
        , COALESCE( ( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (t1.is_rejected = 'false') THEN t1.consumer_uuid ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0
        , COALESCE( ( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (t1.is_rejected = 'false') THEN t1.consumer_uuid ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (is_rejected = 'false') THEN t1.merchant_order_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*60*60*1000) AND (is_rejected = 'false') THEN t1.merchant_order_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (is_rejected = 'false') THEN t1.merchant_order_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (is_rejected = 'false') THEN t1.merchant_order_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (is_rejected = 'false') THEN t1.merchant_order_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (is_rejected = 'false') THEN t1.merchant_order_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0
        FROM sandbox_analytics_us.tmp_feature_audit_feature_event_norm_profile t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_norm_profile t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2
);


----- Default and missing rate checking for features -----
SELECT
       par_process_date
       , COUNT(1) AS records_cnt
       , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0
           FROM sandbox_analytics_us.tmp_feature_audit_feature_value_norm_profile
group by 1
order by 1
;

----- match rate between feature value and simulated value -----
SELECT
    t1.par_process_date
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h2_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h6_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_normalized_profile_hash_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h2_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h6_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_normalized_profile_hash_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h2_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h6_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_normalized_profile_hash_d3_0
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_norm_profile t1
         INNER JOIN sandbox_analytics_us.tmp_feature_audit_feature_simulated_norm_profile t2
                    ON t1.entity_id = t2.entity_id
                        AND t1.checkpoint_time = t2.checkpoint_time
GROUP BY 1
;
