# -*- coding: utf-8 -*-
# @Time    : 2023/4/9 11:28
# @Author  : Euclid-Jie
# @File    : get_date_test.py
import json
from Utils import get_table_info, get_data

# for tabelName in tableInfo.keys():
#     print(json.dumps(get_table_info(tabelName), ensure_ascii=False, indent=2))
tableName = 'EquDiv_info'
print(get_data(tableName).info())
print(json.dumps(get_table_info(tableName), ensure_ascii=False, indent=2))
