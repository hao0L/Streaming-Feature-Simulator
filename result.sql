----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_order_attempt;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_order_attempt DISTKEY(entity_id) AS (
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
    WHERE par_region = 'GB'
      AND par_process_date BETWEEN '2022-04-08' AND '2022-04-10'
      AND key_checkpoint = 'CPE_CHECKOUT_CONFIRM'
)
    SELECT
           par_process_date
           , checkpoint_time
           , key_consumer_id AS consumer_uuid
           , key_consumer_id AS entity_id
----- adding feature extractions for feature audit use case ----
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_attmpt_cc_name_cnt_h12_0', TRUE) AS sp_c_attmpt_cc_name_cnt_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_attmpt_cc_name_cnt_h24_0', TRUE) AS sp_c_attmpt_cc_name_cnt_h24_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_attmpt_cc_name_cnt_d3_0', TRUE) AS sp_c_attmpt_cc_name_cnt_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_attmpt_cc_name_cnt_d7_0', TRUE) AS sp_c_attmpt_cc_name_cnt_d7_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0', TRUE) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0', TRUE) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0', TRUE) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0', TRUE) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0', TRUE) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0', TRUE) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0', TRUE) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0', TRUE) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0
           FROM tmp_source
    WHERE IS_VALID_JSON(feature_map)
    ORDER BY entity_id
    )
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_order_attempt;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_order_attempt DISTKEY(entity_id) AS (
    SELECT
         key_event_info_event_time event_time  --## event_time ##
         , consumer_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , consumer_uuid AS entity_id    --## entity_id ##
         , payment_card_name
         , order_transaction_id
         , status_reason
         --- always use event table as the main table
    FROM green.raw_c_e_order
    WHERE par_region = 'GB'
      AND par_process_date BETWEEN '2022-04-01' AND '2022-04-10'
      ORDER BY entity_id
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_order_attempt;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_order_attempt DISTKEY(entity_id) AS (

    SELECT t2.entity_id
        , t2.checkpoint_time
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) THEN payment_card_name::VARCHAR END), 0) AS sp_c_attmpt_cc_name_cnt_h12_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) THEN payment_card_name::VARCHAR END), 0) AS sp_c_attmpt_cc_name_cnt_h24_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) THEN payment_card_name::VARCHAR END), 0) AS sp_c_attmpt_cc_name_cnt_d3_0
        , COALESCE( COUNT(DISTINCT  CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) THEN payment_card_name::VARCHAR END), 0) AS sp_c_attmpt_cc_name_cnt_d7_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (status_reason = 'INVALID_PAYMENT_DETAILS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (status_reason = 'INVALID_PAYMENT_DETAILS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (status_reason = 'INVALID_PAYMENT_DETAILS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (status_reason = 'INVALID_PAYMENT_DETAILS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (status_reason = 'INSUFFICIENT_FUNDS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (status_reason = 'INSUFFICIENT_FUNDS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (status_reason = 'INSUFFICIENT_FUNDS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (status_reason = 'INSUFFICIENT_FUNDS') THEN order_transaction_id ::VARCHAR END), 0) AS sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0
        FROM sandbox_analytics_us.tmp_feature_audit_feature_event_order_attempt t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_order_attempt t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2
);


----- Default and missing rate checking for features -----
SELECT
       par_process_date
       , COUNT(1) AS records_cnt
       , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_h12_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_attmpt_cc_name_cnt_h12_0
       , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_h12_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_attmpt_cc_name_cnt_h12_0
           , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_h24_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_attmpt_cc_name_cnt_h24_0
       , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_h24_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_attmpt_cc_name_cnt_h24_0
           , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_d3_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_attmpt_cc_name_cnt_d3_0
       , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_d3_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_attmpt_cc_name_cnt_d3_0
           , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_d7_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_attmpt_cc_name_cnt_d7_0
       , 1.0 * COUNT(CASE WHEN sp_c_attmpt_cc_name_cnt_d7_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_attmpt_cc_name_cnt_d7_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0
           , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0 = '' THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_missing_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0
       , 1.0 * COUNT(CASE WHEN sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0 in ('-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_default_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0
           FROM sandbox_analytics_us.tmp_feature_audit_feature_value_order_attempt
group by 1
order by 1
;

----- match rate between feature value and simulated value -----
SELECT
    t1.par_process_date
, COUNT(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_h12_0,'') > 0 THEN 1 END) cnt_sp_c_attmpt_cc_name_cnt_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_h12_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_attmpt_cc_name_cnt_h12_0,'')::DECIMAL(18,2) - t2.sp_c_attmpt_cc_name_cnt_h12_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_h12_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_attmpt_cc_name_cnt_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_h24_0,'') > 0 THEN 1 END) cnt_sp_c_attmpt_cc_name_cnt_h24_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_h24_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_attmpt_cc_name_cnt_h24_0,'')::DECIMAL(18,2) - t2.sp_c_attmpt_cc_name_cnt_h24_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_h24_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_attmpt_cc_name_cnt_h24_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_d3_0,'') > 0 THEN 1 END) cnt_sp_c_attmpt_cc_name_cnt_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_d3_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_attmpt_cc_name_cnt_d3_0,'')::DECIMAL(18,2) - t2.sp_c_attmpt_cc_name_cnt_d3_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_d3_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_attmpt_cc_name_cnt_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_d7_0,'') > 0 THEN 1 END) cnt_sp_c_attmpt_cc_name_cnt_d7_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_d7_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_attmpt_cc_name_cnt_d7_0,'')::DECIMAL(18,2) - t2.sp_c_attmpt_cc_name_cnt_d7_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_attmpt_cc_name_cnt_d7_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_attmpt_cc_name_cnt_d7_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_invalid_pymnt_ordr_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_invalid_pymnt_ordr_h24_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_invalid_pymnt_ordr_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_invalid_pymnt_ordr_d7_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h12_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_h24_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d3_0
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0,'') > 0 THEN 1 END) cnt_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0,'') > 0 AND
                    ABS(NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0,'')::DECIMAL(18,2) - t2.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0::DECIMAL(18,2)) < 1
                              THEN 1 ELSE 0 END) / NULLIF(SUM(CASE
                        WHEN NULLIF(t1.sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0,'') > 0
                            THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decl_topaz_insffcnt_fund_ordr_cnt_d7_0
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_order_attempt t1
         INNER JOIN sandbox_analytics_us.tmp_feature_audit_feature_simulated_order_attempt t2
                    ON t1.entity_id = t2.entity_id
                        AND t1.checkpoint_time = t2.checkpoint_time
GROUP BY 1
;
