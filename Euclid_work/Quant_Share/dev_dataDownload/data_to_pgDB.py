"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/16 9:59
# @Author  : Euclid-Jie
# @File    : data_to_pgDB.py
# @Desc    : 将原本的gm本地h5文件写入postgres数据库
"""
# 连接数据库
from sqlalchemy import create_engine
import pandas as pd
from Euclid_work.Quant_Share import get_data

engine = create_engine("postgresql+psycopg2://postgres:abc123456@localhost:5432/QS")


def write_df_to_pgDB(df, **kwargs):
    pd.io.sql.to_sql(
        df,
        kwargs.get("table_name", "demo"),
        kwargs.get("engine"),
        index=False,
        schema="public",
        if_exists="append",
    )


tableNameList = [
    "share_change",
    "balance_sheet",
    "deriv_finance_indicator",
    "fundamentals_balance",
    "fundamentals_income",
    "fundamentals_cashflow",
    "trading_derivative_indicator",
    "future_daily",
    "continuous_contracts",
    "symbol_industry",
    "gmData_history",
    "gmData_bench_price",
]

for tableName in tableNameList:
    print("开始导入 {}".format(tableName))
    data = get_data(tableName)
    write_df_to_pgDB(data, table_name=tableName, engine=engine)
