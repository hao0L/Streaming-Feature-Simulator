from streaming_feature_simulator.features import SlidingWindowFeatureGroup, LatestFeatureGroup
import json
import os


# input relative file path
def sql_generator(json_file_path):
    with open(json_file_path) as f:
        feature_config = json.load(f)

        if feature_config['feature_type'] == 'sliding_window':
            sql = SlidingWindowFeatureGroup(feature_config).to_sql()
        elif feature_config['feature_type'] == 'latest':
            sql = LatestFeatureGroup(feature_config).to_sql()
    return sql
