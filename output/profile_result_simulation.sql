----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_profile_change;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_profile_change DISTKEY(entity_id) AS (
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
    WHERE par_region in ('US' , 'AU', 'GB')
      AND par_process_date BETWEEN '2022-07-01' AND '2022-07-01'
      AND key_checkpoint = 'CHECKOUT_CONFIRM'
)
    SELECT
            par_region
           , par_process_date
           , checkpoint_time
           , key_consumer_id AS consumer_uuid
           , key_consumer_id AS entity_id
----- adding feature extractions for feature audit use case ----
           FROM tmp_source
    )
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_profile_change;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_profile_change DISTKEY(entity_id) AS (
    SELECT
         EXTRACT('epoch' FROM event_info_event_time)::BIGINT *1000 AS event_time  --## event_time ##
         , consumer_consumer_uuid AS consumer_uuid --## consumer_uuid ##
         , consumer_consumer_uuid AS entity_id    --## entity_id ##
         , consumer_consumer_uuid
         , event_type
         --- always use event table as the main table
    FROM red.raw_c_e_consumer_profile_change
    WHERE par_region in ('US' , 'AU', 'GB')
      AND par_process_date BETWEEN '2022-06-24' AND '2022-07-01'
    ORDER BY entity_id
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_profile_change;
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_profile_change DISTKEY(entity_id) AS (

    SELECT t2.par_region
        , t2.entity_id
        , t2.checkpoint_time
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) THEN consumer_consumer_uuid::VARCHAR END), 0) AS sp_c_profile_action_cnt_all_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (event_type = 'ADD_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_add_card_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (event_type = 'DELETE_CARD') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_delete_card_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (event_type = 'UPDATE_ADDRESS') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_address_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (event_type = 'UPDATE_PHONE') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_phone_d7
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 30*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_m30
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*60*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_h1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 6*60*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_h6
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 1*24*60*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_d1
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 2*24*60*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_d2
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 3*24*60*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_d3
        , COALESCE( COUNT( CASE WHEN (t2.checkpoint_time - t1.event_time BETWEEN 0 AND 7*24*60*60*1000) AND (event_type = 'UPDATE_EMAIL') THEN consumer_consumer_uuid ::VARCHAR END), 0) AS sp_c_profile_action_cnt_update_email_d7
        FROM sandbox_analytics_us.tmp_feature_audit_feature_event_profile_change t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_profile_change t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2, 3
);

