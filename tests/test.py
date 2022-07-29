from streaming_feature_simulator.sql_generator import sql_generator

test_input = '/Users/haoliu/github/Streaming_Feature_Simulator/streaming_feature_simulator/config/sliding_window_profile_change.json'
sql = sql_generator(test_input, save_path='/Users/haoliu/github/Streaming_Feature_Simulator/output/profile_result_simulation.sql')
print(sql)

