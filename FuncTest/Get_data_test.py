
import sys
sys.path.append('../')
from Euclid_work.Quant_Share import get_data, get_table_info
import json

# tableName = 'continuous_contracts'
# print(get_data(tableName, begin='20160301', end='20221231').info())
# print(json.dumps(get_table_info(tableName), ensure_ascii=False, indent=2))

tableName = 'future_daily'
# print(get_data(tableName, begin='20160301', end='20221231').info())
# print(json.dumps(get_table_info(tableName), ensure_ascii=False, indent=2))
print(get_data(tableName, begin='20210301', end='20221231').head())
