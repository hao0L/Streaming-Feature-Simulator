from streaming_feature_simulator.sql_generator import sql_generator

test_input = '/Users/haoliu/github/Streaming_Feature_Simulator/streaming_feature_simulator/config/example_tiny_decision_record.json'
sql = sql_generator(test_input, save_path='./output/tiny_decision_record_result.sql')
print(sql)

