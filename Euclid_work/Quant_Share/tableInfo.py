"""
# -*- coding: utf-8 -*-
# @Time    : 2023/4/20 14:59
# @Author  : Euclid-Jie
# @File    : tableInfo.py
"""
import json
import os

InfoTable = {
    'IndustryID_info': {
        'assets': 'info',
        'description': 'DataAPI.IndustryGet',
    },
    'SecID_E_info': {
        'assets': 'info',
        'description': 'DataAPI.SecIDGet, assetClass=E',
    },
    'SecID_IDX_info': {
        'assets': 'info',
        'description': 'DataAPI.SecIDGet, assetClass=IDX, exchangeCD=[XSHE, HSHG]',
    },
    'SysCode_info': {
        'assets': 'info',
        'description': 'DataAPI.SysCodeGet',
    },
    'PartyID_info': {
        'assets': 'info',
        'description': 'DataAPI.PartyIDGet',
    },
    'bench_info': {
        'assets': 'info',
        'description': '',
    },
    'EquDiv_info': {
        'assets': 'info',
        'description': 'DataAPI.EquDivGet ',
    }
}

gmStock = {
    'bench_price': {
        'assets': 'stock',
        'description': '',
        'date_column': 'trade_date',
        'ticker_column': 'symbol'
    },
    'stock_price': {
        'assets': 'stock',
        'description': '',
        'date_column': 'trade_date',
        'ticker_column': 'symbol'
    },
}

dataYesStock = {
    'HKshszHold': {
        'assets': 'stock',
        'description': '沪深港通持股记录',
        'date_column': 'endDate',
        'ticker_column': 'ticker'
    },
    'MktEqud': {
        'assets': 'stock',
        'description': '沪深股票日行情',
        'date_column': 'tradeDate',
        'ticker_column': 'ticker'
    },
    'MktIdx': {
        'assets': 'stock',
        'description': '指数日行情',
        'date_column': 'tradeDate',
        'ticker_column': 'ticker',
    },

    # 'industry_info': {
    #     'description': '',
    #     'date_column': 'tradeDate',
    #     'ticker_column': 'aiq_ticker'
    # }
    'MktLimit': {
        'assets': 'stock',
        'description': '沪深涨跌停限制',
        'date_column': 'tradeDate',
        'ticker_column': 'ticker'
    },
    'ResConIndex': {
        'assets': 'stock',
        'description': '指数一致预期数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'indexCode'
    },
    'ResConIndexFy12': {
        'assets': 'stock',
        'description': '指数一致预期动态预测数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'indexCode'
    },
    'ResConIndustryCitic': {
        'assets': 'stock',
        'description': '中信行业一致预期数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'induID'
    },
    'ResConIndustryCiticFy12': {
        'assets': 'stock',
        'description': '中信行业一致预期动态预测数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'induID'
    },
    'ResConIndustrySw': {
        'assets': 'stock',
        'description': '申万行业一致预期数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'induID'
    },
    'ResConIndustrySwFy12': {
        'assets': 'stock',
        'description': '申万行业一致预期动态预测数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'induID'
    },
    'ResConSecReportHeat': {
        'assets': 'stock',
        'description': '个股研报热度统计数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'secCode'
    },
    'ResConSecFy12': {
        'assets': 'stock',
        'description': '个股一致预期动态预测数据表',
        'date_column': 'repForeDate',
        'ticker_column': 'secCode'
    },
    'ResConSecCoredata': {
        'assets': 'stock',
        'description': '个股一致预期核心表',
        'date_column': 'repForeDate',
        'ticker_column': 'secCode'
    },
    'ResConSecTarpriScore': {
        'assets': 'stock',
        'description': '个股一致预期目标价与评级表',
        'date_column': 'repForeDate',
        'ticker_column': 'secCode'
    },
    'ResConSecCorederi': {
        'assets': 'stock',
        'description': '个股一致预期核心加工表',
        'date_column': 'repForeDate',
        'ticker_column': 'secCode'
    },
    'FdmtDerPit': {
        'assets': 'stock',
        'description': '财务衍生数据 (Point in time)',
        'date_column': 'publishDate',
        'ticker_column': 'ticker'
    },
    'RMExposureDay': {
        'assets': 'stock',
        'description': '因子暴露表',
        'date_column': 'tradeDate',
        'ticker_column': 'ticker'
    },
}
gmFuture = {
    'Broker_Data': {
        'assets': 'future',
        'description': '',
        'date_column': 'date',
        'ticker_column': ''
    },
    'Price_Volume_Data/main': {
        'assets': 'future',
        'description': '',
        'date_column': 'bob',
        'ticker_column': ''
    },
    'Price_Volume_Data/submain': {
        'assets': 'future',
        'description': '',
        'date_column': 'bob',
        'ticker_column': ''
    },
}

# 针对D:\Share\Stk_Data\gm 下的表
# 从gmStockFactor_tableInfo.json中读取
current_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(current_dir, 'gmStockFactor_tableInfo.json'), 'r') as f:
    gmStockFactor = json.load(f)  # 此时a是一个字典对象

tableInfo = {}
for table in ['InfoTable', 'gmStock', 'dataYesStock', 'gmFuture', 'gmStockFactor']:
    _table = eval(table)
    for key, value in _table.items():
        _table[key]['tableSource'] = table
    tableInfo.update(_table)