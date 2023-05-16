import sys
sys.path.append('../')
from Quant_Share import get_data, get_table_info, tradeDate_info
import json
tableName = 'MktEqud'
print(get_data(tableName, begin='20210301', end='20221231').info())
print(json.dumps(get_table_info(tableName), ensure_ascii=False, indent=2))
