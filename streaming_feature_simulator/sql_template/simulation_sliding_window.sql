    SELECT t2.entity_id
        , t2.checkpoint_time
        {% for feature in FEATURES_SQL_EXPRESSIONS -%}
        , {{feature}}
        {% endfor -%}
    FROM sandbox_analytics_us.tmp_feature_audit_feature_event_{{TABLE_POSTFIX}} t1
    RIGHT JOIN sandbox_analytics_us.tmp_feature_audit_feature_value_{{TABLE_POSTFIX}} t2
        ON t1.entity_id = t2.entity_id
        AND t1.event_time < t2.checkpoint_time
    GROUP BY 1, 2