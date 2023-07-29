"""
# -*- coding: utf-8 -*-
# @Time    : 2023/4/20 14:59
# @Author  : Euclid-Jie
# @File    : TableInfo.py
# @desc    : assets用于指派不同的get_data逻辑, 如果不指定date_column, ticker_column说明该表不支持date索引, ticker索引
"""
import json
import os
from typing import Dict


InfoTable: Dict = {
    "SecID_E_info": {
        "assets": "info",
        "description": "DataAPI.SecIDGet, assetClass=E",
    },
    "SecID_IDX_info": {
        "assets": "info",
        "description": "DataAPI.SecIDGet, assetClass=IDX, exchangeCD=[XSHE, HSHG]",
    },
    "SysCode_info": {
        "assets": "info",
        "description": "DataAPI.SysCodeGet",
    },
    "PartyID_info": {
        "assets": "info",
        "description": "DataAPI.PartyIDGet",
    },
    "bench_info": {
        "assets": "info",
        "description": "",
    },
    "EquDiv_info": {
        "assets": "info",
        "description": "DataAPI.EquDivGet",
    },
    "TradeCal": {
        "assets": "info",
        "description": "DataAPI.TradeCalGet",
    },
}

gmStock = {
    "bench_price": {
        "assets": "stock",
        "description": "",
        "date_column": "trade_date",
        "ticker_column": "symbol",
    },
    "stock_price": {
        "assets": "stock",
        "description": "",
        "date_column": "trade_date",
        "ticker_column": "symbol",
    },
}

gmFuture = {
    "Broker_Data": {
        "assets": "future",
        "description": "",
        "date_column": "date",
        "ticker_column": "",
    },
    "Price_Volume_Data/main": {
        "assets": "future",
        "description": "",
        "date_column": "bob",
        "ticker_column": "",
    },
    "Price_Volume_Data/submain": {
        "assets": "future",
        "description": "",
        "date_column": "bob",
        "ticker_column": "",
    },
}


current_dir = os.path.abspath(os.path.dirname(__file__))
with open(
    os.path.join(current_dir, "dev_files/dataYesStockData_tableInfo.json"),
    "r",
    encoding="utf-8",
) as f:
    dataYesStock = json.load(f)
# 针对D:\Share\Stk_Data\gm 下的表
# 从gmStockFactor_tableInfo.json中读取
with open(
    os.path.join(current_dir, "dev_files/gmStockFactor_tableInfo.json"), "r"
) as f:
    gmStockFactor = json.load(f)
with open(os.path.join(current_dir, "dev_files/gmStockData_tableInfo.json"), "r") as f:
    gmStockData = json.load(f)
with open(os.path.join(current_dir, "dev_files/emData_tableInfo.json"), "r") as f:
    emData = json.load(f)
with open(os.path.join(current_dir, "dev_files/jointquant_tableInfo.json"), "r") as f:
    jointquant_prepare = json.load(f)
with open(os.path.join(current_dir, "dev_files/jointquant_factor.json"), "r") as f:
    jointquant_factor = json.load(f)

tableInfo: Dict = {}
for table in [
    "InfoTable",
    "gmStock",
    "dataYesStock",
    "gmFuture",
    "gmStockFactor",
    "gmStockData",
    "emData",
    "jointquant_prepare",
    "jointquant_factor"
]:
    _table = eval(table)
    if table == "gmStockFactor":
        new_table = {}
        for key, value in _table.items():
            new_table[key + "_original"] = _table[key]
            new_table[key + "_original"]["tableSource"] = table
        _table = new_table
    else:
        for key, value in _table.items():
            _table[key]["tableSource"] = table

    tableInfo.update(_table)
