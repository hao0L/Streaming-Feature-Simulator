----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_risk_actions;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_risk_actions DISTKEY(entity_id) AS (
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
    WHERE par_region = 'AU'
      AND par_process_date BETWEEN '2022-04-25' AND '2022-04-27'
      AND key_checkpoint = 'CHECKOUT_CONFIRM'
)
    SELECT
           par_process_date
           , checkpoint_time
           , key_consumer_id AS consumer_uuid
           , key_consumer_id AS entity_id
----- adding feature extractions for feature audit use case ----
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_h1', TRUE) AS sp_c_login_2fa_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_h6', TRUE) AS sp_c_login_2fa_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_h12', TRUE) AS sp_c_login_2fa_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_d1', TRUE) AS sp_c_login_2fa_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_d2', TRUE) AS sp_c_login_2fa_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_d3', TRUE) AS sp_c_login_2fa_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_login_2fa_cnt_d7', TRUE) AS sp_c_login_2fa_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_h1', TRUE) AS sp_c_online_decline_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_h6', TRUE) AS sp_c_online_decline_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_h12', TRUE) AS sp_c_online_decline_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_d1', TRUE) AS sp_c_online_decline_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_d2', TRUE) AS sp_c_online_decline_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_d3', TRUE) AS sp_c_online_decline_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_decline_cnt_d7', TRUE) AS sp_c_online_decline_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_h1', TRUE) AS sp_c_instore_decline_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_h6', TRUE) AS sp_c_instore_decline_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_h12', TRUE) AS sp_c_instore_decline_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_d1', TRUE) AS sp_c_instore_decline_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_d2', TRUE) AS sp_c_instore_decline_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_d3', TRUE) AS sp_c_instore_decline_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_decline_cnt_d7', TRUE) AS sp_c_instore_decline_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_h1', TRUE) AS sp_c_online_card_scan_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_h6', TRUE) AS sp_c_online_card_scan_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_h12', TRUE) AS sp_c_online_card_scan_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_d1', TRUE) AS sp_c_online_card_scan_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_d2', TRUE) AS sp_c_online_card_scan_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_d3', TRUE) AS sp_c_online_card_scan_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_card_scan_cnt_d7', TRUE) AS sp_c_online_card_scan_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_h1', TRUE) AS sp_c_instore_card_scan_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_h6', TRUE) AS sp_c_instore_card_scan_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_h12', TRUE) AS sp_c_instore_card_scan_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_d1', TRUE) AS sp_c_instore_card_scan_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_d2', TRUE) AS sp_c_instore_card_scan_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_d3', TRUE) AS sp_c_instore_card_scan_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_card_scan_cnt_d7', TRUE) AS sp_c_instore_card_scan_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_h1', TRUE) AS sp_c_online_freeze_cn_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_h6', TRUE) AS sp_c_online_freeze_cn_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_h12', TRUE) AS sp_c_online_freeze_cn_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_d1', TRUE) AS sp_c_online_freeze_cn_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_d2', TRUE) AS sp_c_online_freeze_cn_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_d3', TRUE) AS sp_c_online_freeze_cn_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_freeze_cn_d7', TRUE) AS sp_c_online_freeze_cn_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_h1', TRUE) AS sp_c_instore_freeze_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_h6', TRUE) AS sp_c_instore_freeze_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_h12', TRUE) AS sp_c_instore_freeze_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_d1', TRUE) AS sp_c_instore_freeze_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_d2', TRUE) AS sp_c_instore_freeze_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_d3', TRUE) AS sp_c_instore_freeze_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_freeze_cnt_d7', TRUE) AS sp_c_instore_freeze_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_h1', TRUE) AS sp_c_online_suspend_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_h6', TRUE) AS sp_c_online_suspend_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_h12', TRUE) AS sp_c_online_suspend_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_d1', TRUE) AS sp_c_online_suspend_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_d2', TRUE) AS sp_c_online_suspend_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_d3', TRUE) AS sp_c_online_suspend_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_online_suspend_cnt_d7', TRUE) AS sp_c_online_suspend_cnt_d7
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_h1', TRUE) AS sp_c_instore_suspend_cnt_h1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_h6', TRUE) AS sp_c_instore_suspend_cnt_h6
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_h12', TRUE) AS sp_c_instore_suspend_cnt_h12
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_d1', TRUE) AS sp_c_instore_suspend_cnt_d1
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_d2', TRUE) AS sp_c_instore_suspend_cnt_d2
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_d3', TRUE) AS sp_c_instore_suspend_cnt_d3
           , JSON_EXTRACT_PATH_TEXT(feature_map, 'sp_c_instore_suspend_cnt_d7', TRUE) AS sp_c_instore_suspend_cnt_d7
           FROM tmp_source
    WHERE IS_VALID_JSON(feature_map)
    ORDER BY entity_id
    )
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_risk_actions;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_risk_actions DISTKEY(entity_id) AS (
SELECT
         date_part(epoch, event_info_event_time) * 1000 ::BIGINT AS event_time  --## event_time ##
         , key_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , consumer_uuid AS entity_id    --## entity_id ##
         , key_consumer_uuid
         , checkpoint
         , actions
         --- always use event table as the main table
    FROM green.raw_c_e_udp_tiny_decision_record
    WHERE par_region = 'AU'
      AND par_process_date BETWEEN '2022-04-18' AND '2022-04-27'
    ORDER BY entity_id
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_risk_actions;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_risk_actions DISTKEY(entity_id) AS (

    SELECT t2.entity_id
        , t2.checkpoint_time
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('LOGIN') AND lower(actions) like '%login_2fa_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_login_2fa_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_decline_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%decline%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_decline_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_card_scan_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%card_scan_required%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_card_scan_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_freeze_cn_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%frozen%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_freeze_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('CHECKOUT_CONFIRM') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_online_suspend_cnt_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 12*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_h12
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (checkpoint in ('BARCODE_GENERATION','BARCODE_REDEMPTION') AND lower(actions) like '%suspended%' ) THEN key_consumer_uuid ::VARCHAR END), 0) AS sp_c_instore_suspend_cnt_d7
        FROM sandbox_analytics_us.tmp_feature_audit_feature_event_risk_actions t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_risk_actions t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2
);


----- Default and missing rate checking for features -----
SELECT
       par_process_date
       , COUNT(1) AS records_cnt
       , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_login_2fa_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_login_2fa_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_online_decline_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_decline_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_instore_decline_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_decline_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_online_card_scan_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_card_scan_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_instore_card_scan_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_card_scan_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_h1
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_h6
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_h12
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_d1
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_d2
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_d3
           , 1.0 * COUNT(CASE WHEN sp_c_online_freeze_cn_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_freeze_cn_d7
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_instore_freeze_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_freeze_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_online_suspend_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_online_suspend_cnt_d7
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_h1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_h1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_h6 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_h6
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_h12 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_h12
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_d1 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_d1
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_d2 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_d2
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_d3 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_d3
           , 1.0 * COUNT(CASE WHEN sp_c_instore_suspend_cnt_d7 NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_sp_c_instore_suspend_cnt_d7
           FROM sandbox_analytics_us.tmp_feature_audit_feature_value_risk_actions
group by 1
order by 1
;

----- match rate between feature value and simulated value -----
SELECT
    t1.par_process_date
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_login_2fa_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_login_2fa_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_login_2fa_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_login_2fa_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_login_2fa_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_decline_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_decline_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_online_decline_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_decline_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_decline_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_decline_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_decline_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_instore_decline_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_decline_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_decline_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_card_scan_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_card_scan_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_online_card_scan_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_card_scan_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_card_scan_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_card_scan_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_card_scan_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_instore_card_scan_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_card_scan_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_card_scan_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_h1,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_h6,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_h12,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_d1,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_d2,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_d3,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_freeze_cn_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_freeze_cn_d7,'')::DECIMAL(18,2) - t2.sp_c_online_freeze_cn_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_freeze_cn_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_freeze_cn_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_freeze_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_freeze_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_instore_freeze_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_freeze_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_freeze_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_online_suspend_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_online_suspend_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_online_suspend_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_online_suspend_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_online_suspend_cnt_d7
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_h1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_h1,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_h1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_h1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h6,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_h6
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h6,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_h6,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_h6::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h6,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_h6
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h12,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_h12
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h12,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_h12,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_h12::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_h12,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_h12
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d1,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_d1
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d1,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_d1,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_d1::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d1,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_d1
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d2,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_d2
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d2,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_d2,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_d2::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d2,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_d2
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d3,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_d3
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d3,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_d3,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_d3::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d3,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_d3
, COUNT(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d7,'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_sp_c_instore_suspend_cnt_d7
    , 1.0 * SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d7,'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.sp_c_instore_suspend_cnt_d7,'')::DECIMAL(18,2) - t2.sp_c_instore_suspend_cnt_d7::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.sp_c_instore_suspend_cnt_d7,'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_sp_c_instore_suspend_cnt_d7
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_risk_actions t1
         INNER JOIN sandbox_analytics_us.tmp_feature_audit_feature_simulated_risk_actions t2
                    ON t1.entity_id = t2.entity_id
                        AND t1.checkpoint_time = t2.checkpoint_time
GROUP BY 1
;
