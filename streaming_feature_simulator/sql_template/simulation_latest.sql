WITH tmp_source AS (
    SELECT t2.entity_id
        , t2.checkpoint_time
        , t1.event_time
        , ROW_NUMBER() OVER (PARTITION BY t2.entity_id ORDER BY t1.event_time DESC) event_time_rank
    FROM sandbox_analytics_us.tmp_feature_audit_feature_event_{{TABLE_POSTFIX}} t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}} t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    {% if FILTER_EXPRESSION != '' -%}
    WHERE {{FILTER_EXPRESSION}}
    {% endif -%}
    )
    SELECT
        t1.entity_id
        , t1.event_time
        , t2.checkpoint_time
        {% for feature in FEATURES_SQL_EXPRESSIONS -%}
        , {{feature}}
        {% endfor -%}
    FROM sandbox_analytics_us.tmp_feature_audit_feature_event_{{TABLE_POSTFIX}} t1
    INNER JOIN tmp_source t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time = t2.event_time
    WHERE t2.event_time_rank = 1