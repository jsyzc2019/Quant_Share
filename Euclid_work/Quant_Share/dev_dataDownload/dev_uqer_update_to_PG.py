"""
# -*- coding: utf-8 -*-
# @Time    : 2023/8/2 22:21
# @Author  : Euclid-Jie
# @File    : dev_uqer_update_to_PG.py
"""
from Euclid_work.Quant_Share.warehouse import *
from Euclid_work.Quant_Share.dev_dataDownload.meta_uqer_dataDownLoad import *

# stockNumList = pd.read_sql(
#     "select sec_id  from stock_info where delisted_date >= '2015-01-01'",
#     con=postgres_engine(),
# )["sec_id"].values.tolist()
#
# # 获取数据库中已有数据
# begin = pd.read_sql(
#     "select max(Date(tradedate)) as date from uqer_mkt_equd_adj",
#     con=postgres_engine(),
# ).values[0][0]

data = mIdxCloseWeight(begin="2015-01-01", end=date.today().strftime("%Y-%m-%d"), ticker=["000300", "000852", "000905"])
postgres_write_data_frame(
    clean_data_frame_to_postgres(data, lower=True),
    table_name="uqer_midx_close_weight",
    update=True,
    unique_index=["ticker", "effdate", "constickersymbol"],
    record_time=True
)

postgres_cur_execute(
    database="QS",
    sql_text="""
    UPDATE uqer_midx_close_weight
    SET update_time = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai'""",
)
