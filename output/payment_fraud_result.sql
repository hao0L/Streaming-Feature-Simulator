----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_payment_fraud;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_payment_fraud DISTKEY(entity_id) AS (
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
         , key_is_rejected
         , (CASE
                    WHEN NOT is_valid_json(rules_variables)
                        THEN rtrim(regexp_substr(rules_variables, '^.*[,]'),',') || '}'
                    ELSE rules_variables END) AS feature_map
    FROM red.raw_c_e_fc_decision_record
    WHERE par_region = 'AU'
      AND par_process_date BETWEEN '2022-07-22' AND '2022-07-25'
      AND key_checkpoint = 'CHECKOUT_CONFIRM'
)
    SELECT
            par_region
           , par_process_date
           , checkpoint_time
           , key_consumer_id AS consumer_uuid
           , key_consumer_id AS entity_id
----- adding feature extractions for feature audit use case ----
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_card_expired_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_card_expired_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_m30', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_m30
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_h1', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_h6', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_h12', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_h24', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h24
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_h48', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h48
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_h72', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h72
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_acct_decl_topaz_credit_card_declined_cnt_d7', TRUE) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_d7
           FROM tmp_source
    WHERE IS_VALID_JSON(feature_map)
    ORDER BY entity_id
    )
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_payment_fraud;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_payment_fraud DISTKEY(entity_id) AS (
    SELECT
         extract(epoch from event_info_event_time)::BIGINT event_time  --## event_time ##
         , key_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , key_consumer_uuid AS entity_id    --## entity_id ##
         , key_consumer_uuid
         , internal_status_code
         , issuer_decline_code
         --- always use event table as the main table
    FROM red.raw_r_e_payment_fraud_info
    WHERE par_region = 'AU'
      AND par_process_date BETWEEN '2022-07-15' AND '2022-07-25'
      ORDER BY entity_id
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_payment_fraud;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_payment_fraud DISTKEY(entity_id) AS (

    SELECT t2.entity_id
        , t2.checkpoint_time
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (issuer_decline_code = 21) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (issuer_decline_code = 41) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (issuer_decline_code = 4) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (issuer_decline_code not in (21, 41, 4)) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_NUMBER') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_EXPIRY_DATE') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (internal_status_code = 'INVALID_CREDIT_CARD_CVV') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (internal_status_code = 'CARD_EXPIRED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_card_expired_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 24*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h24
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 48*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h48
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 72*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_h72
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (internal_status_code = 'CREDIT_CARD_DECLINED') THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_acct_decl_topaz_credit_card_declined_cnt_d7
        FROM sandbox_analytics_us.tmp_feature_audit_feature_event_payment_fraud t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_payment_fraud t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2
);


----- Default and missing rate checking for features -----
SELECT
       par_process_date
       , COUNT(1) AS records_cnt
       , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_card_expired_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_card_expired_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_m30 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_m30
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_h24 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_h24
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_h48 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_h48
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_h72 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_h72
           , 1.0 * COUNT(CASE WHEN sp_c_acct_decl_topaz_credit_card_declined_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_acct_decl_topaz_credit_card_declined_cnt_d7
           FROM sandbox_analytics_us.tmp_feature_audit_feature_value_payment_fraud
group by 1
order by 1
;

----- match rate between feature value and simulated value -----
SELECT
    t1.par_process_date
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_pymnt_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_insffcnt_fund_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_breach_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_fraud_other_decl_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cc_num_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_expire_dt_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_invalid_cvv_pymt_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_card_expired_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_card_expired_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_card_expired_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_card_expired_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_m30,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_m30
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_m30,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_m30,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_m30::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_m30,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_m30
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h24,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_h24
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h24,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h24,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_h24::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h24,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_h24
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h48,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_h48
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h48,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h48,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_h48::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h48,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_h48
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h72,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_h72
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h72,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h72,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_h72::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_h72,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_h72
, COUNT(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_acct_decl_topaz_credit_card_declined_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_acct_decl_topaz_credit_card_declined_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_acct_decl_topaz_credit_card_declined_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_acct_decl_topaz_credit_card_declined_cnt_d7
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_payment_fraud t1
         INNER JOIN sandbox_analytics_us.tmp_feature_audit_feature_simulated_payment_fraud t2
                    ON t1.entity_id = t2.entity_id
                        AND t1.checkpoint_time = t2.checkpoint_time
GROUP BY 1
;
