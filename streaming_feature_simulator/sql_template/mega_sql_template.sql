----- Extract all checkpoint calls during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}};
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}} DISTKEY(entity_id) AS (
{% include CHECKPOINT_TABLE %}
    SELECT
            par_region
           , par_process_date
           , checkpoint_time
           , {{CONSUMER_UUID_COLUMN}} AS consumer_uuid
           , {{ENTITY_ID_FROM_REQUEST}} AS entity_id
----- adding feature extractions for feature audit use case ----
           {% if FEATURE_AUDIT -%}
           {% for feature in FEATURES -%}
           , JSON_EXTRACT_PATH_TEXT(feature_map, '{{feature}}', TRUE) AS {{feature}}
           {% endfor -%}
           {%endif -%}
    FROM tmp_source
    {% if FEATURE_AUDIT -%}
    WHERE IS_VALID_JSON(feature_map)
    ORDER BY entity_id
    {%endif -%}
)
;

----- Extract all raw events during simulation period
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_event_{{TABLE_POSTFIX}};
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_event_{{TABLE_POSTFIX}} DISTKEY(entity_id) AS (
{% include EVENT_TABLE %}
);

----- Calculate the simulated feature values and map to entity_id + checkpoint_time -----
DROP TABLE IF EXISTS sandbox_analytics_us.tmp_feature_audit_feature_simulated_{{TABLE_POSTFIX}};
CREATE TABLE sandbox_analytics_us.tmp_feature_audit_feature_simulated_{{TABLE_POSTFIX}} DISTKEY(entity_id) AS (
{% if FEATURE_LATEST -%}
{% include "simulation_latest.sql" %}
{% else %}
{% include "simulation_sliding_window.sql" %}
{% endif -%}
);

{% if FEATURE_AUDIT %}
----- Default and missing rate checking for features -----
SELECT
       par_process_date
       , COUNT(1) AS records_cnt
       {% for feature in FEATURES -%}
       , 1.0 * COUNT(CASE WHEN {{feature}} NOT IN ('', '-999', '-999.0', '0.0', '0') THEN 1 END) /
       NULLIF(COUNT(1),0) AS pct_value_{{feature}}
           {% endfor -%}
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}}
group by 1
order by 1
;

----- match rate between feature value and simulated value -----
SELECT
    t1.par_process_date
{% for feature in FEATURES -%}
    , COUNT(CASE WHEN NULLIF(t1.{{feature}},'')::DECIMAL(18,2) > 0 THEN 1 END) cnt_{{feature}}
    , 1.0 * SUM(CASE WHEN NULLIF(t1.{{feature}},'')::DECIMAL(18,2) > 0
                AND ABS(NULLIF(t1.{{feature}},'')::DECIMAL(18,2) - t2.{{feature}}::DECIMAL(18,2)) < 1
                THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN NULLIF(t1.{{feature}},'')::DECIMAL(18,2) > 0
                                THEN 1 ELSE 0 END), 0) pct_match_{{feature}}
{% endfor -%}
FROM sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}} t1
         INNER JOIN sandbox_analytics_us.tmp_feature_audit_feature_simulated_{{TABLE_POSTFIX}} t2
                    ON t1.entity_id = t2.entity_id
                        AND t1.checkpoint_time = t2.checkpoint_time
GROUP BY 1
;
{%endif %}