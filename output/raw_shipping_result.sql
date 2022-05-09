----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_raw_shipping_v2;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_raw_shipping_v2 DISTKEY(entity_id) AS (
WITH tmp_source AS (
    SELECT par_process_date
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
      AND par_process_date BETWEEN '2022-05-03' AND '2022-05-05'
      AND key_checkpoint = 'CHECKOUT_CONFIRM'
)
    SELECT
           par_process_date
           , checkpoint_time
           , key_consumer_id AS consumer_uuid
           , MD5(LOWER(TRIM(JSON_EXTRACT_PATH_TEXT(request, 'order', 'shipping_address', 'address1', TRUE))||' '||TRIM(json_extract_path_text(request, 'order', 'shipping_address', 'address2', TRUE))||' '||TRIM(json_extract_path_text(request, 'order', 'shipping_address', 'suburb', TRUE))||' '||TRIM(json_extract_path_text(request, 'order', 'shipping_address', 'state', TRUE))||' '||TRIM(json_extract_path_text(request, 'order', 'shipping_address', 'postcode', TRUE))||' '||TRIM(json_extract_path_text(request, 'order', 'shipping_address', 'country_code', TRUE)))) AS entity_id
----- adding feature extractions for feature audit use case ----
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0', TRUE) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0', TRUE) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0', TRUE) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0
           FROM tmp_source
    WHERE IS_VALID_JSON(feature_map)
    ORDER BY entity_id
    )
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_raw_shipping_v2;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_raw_shipping_v2 DISTKEY(entity_id) AS (
    SELECT
         key_event_info_event_time::BIGINT event_time  --## event_time ##
         , consumer_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , MD5(LOWER(TRIM(order_detail_shipping_address_address1)||' '||TRIM(order_detail_shipping_address_address2)||' '||TRIM(order_detail_shipping_address_suburb)||' '||TRIM(order_detail_shipping_address_state)||' '||TRIM(order_detail_shipping_address_postcode)||' '||TRIM(order_detail_shipping_address_country_code))) AS entity_id    --## entity_id ##
         , consumer_total_amount_amount
         , consumer_consumer_uuid
         , key_token
         , status
         --- always use event table as the main table
    FROM red.raw_c_e_order
    WHERE par_region = 'US'
      AND par_process_date BETWEEN '2022-04-30' AND '2022-05-05'
      ORDER BY entity_id
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_raw_shipping_v2;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_raw_shipping_v2 DISTKEY(entity_id) AS (

    SELECT t2.entity_id
        , t2.checkpoint_time
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (status='APPROVED') THEN key_token ::VARCHAR END), 0) AS sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0
        , COALESCE( SUM( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (status='APPROVED') THEN consumer_total_amount_amount ::DECIMAL(18,2) END), 0) AS sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0
        FROM sandbox_analytics_us.tmp_feature_audit_feature_event_raw_shipping_v2 t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_raw_shipping_v2 t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2
);


----- Default and missing rate checking for features -----
SELECT
       par_process_date
       , COUNT(1) AS records_cnt
       , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0
           , 1.0 * COUNT(CASE WHEN sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0
           FROM sandbox_analytics_us.tmp_feature_audit_feature_value_raw_shipping_v2
group by 1
order by 1
;

----- match rate between feature value and simulated value -----
SELECT
    t1.par_process_date
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h6_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d2_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_consumer_cnt_by_raw_shipping_hash_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h6_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d2_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_cnt_by_raw_shipping_hash_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h3_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h6_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d1_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d2_0
, COUNT(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) - t2.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_address_linking_total_success_order_amt_by_raw_shipping_hash_d3_0
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_raw_shipping_v2 t1
         INNER JOIN sandbox_analytics_us.tmp_feature_audit_feature_simulated_raw_shipping_v2 t2
                    ON t1.entity_id = t2.entity_id
                        AND t1.checkpoint_time = t2.checkpoint_time
GROUP BY 1
;
