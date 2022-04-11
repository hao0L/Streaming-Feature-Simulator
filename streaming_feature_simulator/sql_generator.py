from streaming_feature_simulator.features import SlidingWindowFeatureGroup, LatestFeatureGroup
import json
import os


# input absolute file path
def sql_generator(json_file_path, save_sql=True, save_path="./result.sql"):
    with open(json_file_path) as f:
        feature_config = json.load(f)

        if feature_config['feature_type'] == 'sliding_window':
            sql = SlidingWindowFeatureGroup(feature_config).to_sql()
        elif feature_config['feature_type'] == 'latest':
            sql = LatestFeatureGroup(feature_config).to_sql()

    if save_sql:
        with open(save_path, 'w') as f:
            f.write(sql)

    return sql
