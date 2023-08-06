"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/22 21:16
# @Author  : Euclid-Jie
# @File    : base_package.py
# @Desc    : meta_gm_data_update_to_PG用到的常用库
"""
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
from gm.api import *
from Euclid_work.Quant_Share.warehouse import *
from Euclid_work.Quant_Share import (
    format_date,
    patList,
    save_data_h5,
    dataBase_root_path_gmStockFactor,
    stock_info,
    get_tradeDates,
    get_tradeDate,
    bench_info,
)
from tqdm import tqdm
import time
from datetime import date, datetime, timedelta
import logging

# 路径识别
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
grand_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
dev_files_dir = os.path.join(grand_dir, "dev_files")

# Gm登录
with open(os.path.join(current_dir, "token.txt"), "rt", encoding="utf-8") as f:
    token = f.read().strip()
set_token(token)

# 全量股票symbol, 格式为"SHSE.600001"
# symbolList = list(stock_info.symbol.unique())
symbolList = pd.read_sql(
    "select symbol  from stock_info where delisted_date >= '2015-01-01'",
    con=postgres_engine(),
)["symbol"].values
bench_symbol_list = list(set(bench_info["symbol"]))


def logger_update_to_PG(log_file_name: str, **kwargs):
    sub_path = kwargs.get("sub_path", "update_log")
    os.makedirs(sub_path, exist_ok=True)  # if existed, pass

    logging.basicConfig(
        filename="{}/{}_{}.txt".format(
            kwargs.get("sub_path", "update_log"),
            datetime.now().strftime("%Y%m%d_%H%M%S"),
            log_file_name,
        ),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger()
    return logger


def update_time(table_name: str, symbol: str, time_column_name: str, database: str):
    """
    用于向QS_log中的表更新record_time, 无论是否有数据更新, 都应该更新QS_log中的对应表的record_time
    :param table_name: 如果表名中带有大写, 需要两层引号, 如 '"gmData_history_1m"', 如果无大写, 则为‘balance_sheet_latest_info’即可
    :param symbol: 需要更新的symbol
    :param time_column_name:
    :param database
    """
    postgres_cur_execute(
        database=database,
        sql_text="""
        UPDATE {}
        SET {} = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai'
        WHERE symbol='{}'
    """.format(
            table_name, time_column_name, symbol
        ),
    )
