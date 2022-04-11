from streaming_feature_simulator.template import SQL_TEMPLATE
import re
from datetime import datetime, timedelta

AGG_METHOD_MAPPING = {
    'count': 'COUNT',
    'count_distinct': 'COUNT',
    'sum': 'SUM',
    'max': 'MAX',
    'min': 'MIN',
    'avg': 'AVG'
}

IF_DISTINCT_MAPPING = {'count_distinct': 'DISTINCT '}

IF_AUDIT_MAPPING = {'feature_audit': True, 'feature_simulation': False}

IF_LATEST_MAPPING = {'latest': True, 'sliding_window': False}

max_window = 30  # maximum offset time period for event data vs. checkpoint data


class SlidingWindowFeature:
    def __init__(self, feature_name, value, data_type, filter, window, aggfunc):
        self.feature_name = feature_name
        self.value = value
        self.data_type = data_type.upper()
        self.filter = filter
        self.window = window
        self.aggfunc = aggfunc

    @staticmethod
    def window_to_diff_seconds(window):
        matched = re.match("([m|h|d])([\d]+)", window)
        if matched:
            window_unit, window_number = matched.groups()
            window_size_mapping = {'d': '24*60*60*1000', 'h': '60*60*1000', 'm': '60*1000'}

            return "{window_number}*{seconds}".format(
                window_number=window_number,
                seconds=window_size_mapping.get(window_unit, 1)
            )
        else:
            raise Exception(f"Unsupported window: {window}")

    def to_sql_expression(self):

        window_condition = "(t2.checkpoint_time - t1.event_time BETWEEN 0 AND {max_diff_seconds})".format(
            max_diff_seconds=self.window_to_diff_seconds(self.window)
        )

        if self.filter != "":
            value_expr = f"CASE WHEN {window_condition} AND ({self.filter}) THEN {self.value} ::{self.data_type} END"
        else:
            value_expr = f"CASE WHEN {window_condition} THEN {self.value}::{self.data_type} END"

        sql_template = "COALESCE( {agg}({if_distinct} {value_expr}), 0) AS {feature_name}"
        sql = sql_template.format(
            agg=AGG_METHOD_MAPPING.get(self.aggfunc, ""),
            if_distinct=IF_DISTINCT_MAPPING.get(self.aggfunc, ""),
            value_expr=value_expr,
            feature_name=self.feature_name
        )
        return sql


class BaseFeatureGroup:

    def __init__(self, feature_config):
        # create kafka-fields -> {key, column_name, display_name} mapping dict
        # mapping
        self.entity_id_from_request = feature_config['entity_id_from_request']
        self.entity_id = feature_config['entity_id_column']
        self.event_time_key = feature_config['event_time_key']
        self.consumer_uuid_column = feature_config['consumer_uuid_column']

        self.region = feature_config['region']
        self.start_date = feature_config['start_date']
        self.end_date = feature_config['end_date']
        self.checkpoint = feature_config['checkpoint']

        self.feature_type = feature_config.get('feature_type')
        self.features = feature_config['feature_columns']

        self.filters = feature_config['filter_columns']
        self.feature_audit = feature_config['audit_or_simulation']
        self.event_table = feature_config['event_table']
        self.checkpoint_table = feature_config['checkpoint_table']

        self.table_postfix = feature_config['table_postfix']

    def to_sql(self):
        NotImplemented

    def to_flink_sql(self):
        NotImplemented

    def to_yml(self):
        NotImplemented


# Window Type of Calculation
class SlidingWindowFeatureGroup(BaseFeatureGroup):

    def __init__(self, feature_config):
        super().__init__(feature_config)

        self.windows = feature_config['windows']
        self.sql_template = SQL_TEMPLATE['sliding_window']['sql']

    def to_sql(self):
        # create the sliding window sql
        feature_names = []
        feature_sql_expressions = []
        other_column_names = []
        filter_list = [field['column'] for field in self.filters]

        # iterate each feature, and each sliding window to generate Redshift SQL
        for feature_def in self.features:
            value_column_name = feature_def['column_alias'] if feature_def['column_alias'] else feature_def['column']
            data_type = feature_def['data_type']
            aggfunc = feature_def['aggfunc']

            if feature_def['prefix'] is not None and isinstance(feature_def['prefix'], str) and len(
                    feature_def['prefix']) > 0:
                feature_prefix = feature_def['prefix']
            else:
                feature_prefix = f"sp_{aggfunc}_{value_column_name}_by_{self.entity_column_name}"

            filter_expression = feature_def['filter']

            for window in self.windows:
                feature_name_template = f"{feature_prefix}_{window}_0"

                feature_sql_expr = SlidingWindowFeature(
                    value=value_column_name,
                    data_type=data_type,
                    window=window,
                    aggfunc=aggfunc,
                    filter=filter_expression,
                    feature_name=feature_name_template.format(feature_prefix, window)
                ).to_sql_expression()
                feature_sql_expressions.append(feature_sql_expr)
                feature_names.append(feature_name_template.format(feature_prefix, window))

                max_window = max(
                    [re.match("([d])([\d]+)", window).groups()[1] if re.match("([d])([\d]+)", window) else 0])

            # the other column names used in feature values and filters
            event_value_column = f"{feature_def['column']} AS {feature_def['column_alias']}" if feature_def[
                    'column_alias'] else feature_def['column']
            other_column_names.append(event_value_column)

        other_column_names = list(filter(None, list(set(other_column_names)) + (list(set(filter_list)))))

        event_start_date = (
                datetime.strptime(self.start_date, '%Y-%m-%d').date() - timedelta(days=int(max_window))).strftime(
            '%Y-%m-%d')

        # use Jinja to render the template
        sql = self.sql_template.render(
            ENTITY_ID_FROM_REQUEST=self.entity_id_from_request,
            EVENT_TIME_COLUMN_NAME=self.event_time_key,
            ENTITY_COLUMN_NAME=self.entity_id,
            OTHER_COLUMN_NAMES=other_column_names,
            FEATURE_AUDIT=IF_AUDIT_MAPPING.get(self.feature_audit, False),
            FEATURES=feature_names,
            FEATURES_SQL_EXPRESSIONS=feature_sql_expressions,
            REGION=self.region,
            CHECKPOINT=self.checkpoint,
            START_DATE=self.start_date,
            END_DATE=self.end_date,
            EVENT_START_DATE=event_start_date,
            CHECKPOINT_TABLE=self.checkpoint_table,
            EVENT_TABLE=self.event_table,
            CONSUMER_UUID_COLUMN=self.consumer_uuid_column,
            TABLE_POSTFIX=self.table_postfix
        )

        return sql

    def to_yml(self):
        NotImplemented

    def to_flink_sql(self):
        NotImplemented


# Latest Type of Calculation
class LatestFeatureGroup(BaseFeatureGroup):

    def __init__(self, feature_config):
        super().__init__(feature_config)

        self.windows = feature_config['windows']
        self.sql_template = SQL_TEMPLATE['latest']['sql']

    def to_sql(self):
        # create the sliding window sql
        feature_names = []
        feature_sql_expressions = []
        other_column_names = []
        filter_list = [field['column'] for field in self.filters]

        # iterate each feature, and each sliding window to generate Redshift SQL
        for feature_def in self.features:
            value_column_name = feature_def['column_alias'] if feature_def['column_alias'] else feature_def['column']
            data_type = feature_def['data_type'].upper()
            aggfunc = 'latest'

            if feature_def['prefix'] is not None and isinstance(feature_def['prefix'], str) and len(
                    feature_def['prefix']) > 0:
                feature_prefix = feature_def['prefix']
            else:
                feature_prefix = f"sp_{aggfunc}_{value_column_name}_by_{self.entity_id}"

            filter_expression = feature_def['filter']

            feature_name_template = f"{feature_prefix}"
            feature_name = feature_name_template.format(feature_prefix)

            feature_sql_expr = f"{value_column_name}::{data_type} AS {feature_name}"
            feature_sql_expressions.append(feature_sql_expr)
            feature_names.append(feature_name)

            # the other column names used in feature values and filters
            event_value_column = f"{feature_def['column']} AS {feature_def['column_alias']}" if feature_def['column_alias'] else feature_def['column']
            other_column_names.append(event_value_column)

        other_column_names = list(filter(None, list(set(other_column_names)) + (list(set(filter_list)))))
        event_start_date = (
                datetime.strptime(self.start_date, '%Y-%m-%d').date() - timedelta(days=int(max_window))).strftime(
            '%Y-%m-%d')

        # use Jinja to render the template
        sql = self.sql_template.render(
            ENTITY_ID_FROM_REQUEST=self.entity_id_from_request,
            EVENT_TIME_COLUMN_NAME=self.event_time_key,
            ENTITY_COLUMN_NAME=self.entity_id,
            OTHER_COLUMN_NAMES=other_column_names,
            FEATURE_AUDIT=IF_AUDIT_MAPPING.get(self.feature_audit, False),
            FEATURE_LATEST=IF_LATEST_MAPPING.get(self.feature_type, False),
            FEATURES=feature_names,
            FEATURES_SQL_EXPRESSIONS=feature_sql_expressions,
            FILTER_EXPRESSION=filter_expression,
            REGION=self.region,
            CHECKPOINT=self.checkpoint,
            START_DATE=self.start_date,
            END_DATE=self.end_date,
            EVENT_START_DATE=event_start_date,
            CHECKPOINT_TABLE=self.checkpoint_table,
            EVENT_TABLE=self.event_table,
            CONSUMER_UUID_COLUMN=self.consumer_uuid_column,
            TABLE_POSTFIX=self.table_postfix
        )

        return sql

    def to_yml(self):
        NotImplemented

    def to_flink_sql(self):
        NotImplemented
