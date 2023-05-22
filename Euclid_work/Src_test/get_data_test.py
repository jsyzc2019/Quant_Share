
from Euclid_work.Quant_Share import get_data, get_table_info, tradeDate_info
import json
tableName = 'Shanghai_scale_index_daily'
print(get_data(tableName, begin='20210301', end='20221231').info())
print(json.dumps(get_table_info(tableName), ensure_ascii=False, indent=2))
