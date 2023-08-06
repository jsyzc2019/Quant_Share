"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/16 9:59
# @Author  : Euclid-Jie
# @File    : data_to_pgDB.py
# @Desc    : 将原本的gm本地h5文件写入postgres数据库
"""
from Euclid_work.Quant_Share.warehouse import *
from Euclid_work.Quant_Share.EuclidGetData import get_data

tableNameList = [
    "MktEqud",
    "MktLimit",
    "FdmtIndiRtnPit",
    "FdmtIndiPSPit",
    "MktIdx",
    "mIdxCloseWeight",
    "ResConIndex",
    "ResConIndexFy12",
    "ResConIndustryCitic",
    "ResConIndustryCiticFy12",
    "ResConIndustrySw",
    "ResConIndustrySwFy12",
    "ResConSecReportHeat",
    "ResConSecCoredata",
    "ResConSecTarpriScore",
    "ResConSecCorederi"
]

for tableName in tableNameList:
    print("开始导入 {}".format(tableName))
    data = get_data(tableName)
    write_df_to_pgDB(data, table_name="uqer_" + tableName, engine=postgres_engine())
