"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 14:49
# @Author  : Euclid-Jie
# @File    : dev_uqerUpdate.py
"""
from Euclid_work.Quant_Share import get_tradeDate, get_table_info, printJson
from meta_uqer_dataDownLoad import *
from datetime import datetime

# 接口文档 https://mall.datayes.com/mydata/purchasedData
# 根据数据表最后写入的时间做增量更新
tableNameList = [
    "MktEqud",
    "MktLimit",
    "FdmtIndiRtnPit",
    "FdmtIndiPSPit",
    "MktIdx",
    'mIdxCloseWeight'
]
for tableName in tableNameList:
    begin = get_tradeDate(get_table_info(tableName)['Modify Time'], -1)
    print("-*-"*35)
    if begin < get_tradeDate(datetime.now(), -5):
        print("{} update from {} start!".format(tableName, begin.strftime('%Y%m%d')))
        eval(tableName + '_update(upDateBegin=begin)')
    # printJson(get_table_info(tableName))
    else:
        print("{} has been updated at {} start!".format(tableName, begin.strftime('%Y%m%d')))
