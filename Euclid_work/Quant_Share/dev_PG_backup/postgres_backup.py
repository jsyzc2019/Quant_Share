"""
# -*- coding: utf-8 -*-
# @Time    : 2023/8/27 22:38
# @Author  : Euclid-Jie
# @File    : postgres_backup.py
# @Desc    : 更新数据至 "E:\\Share\\Stk_Data\\postgres_backup", 格式为h5
"""
from Euclid_work.Quant_Share.dev_dataDownload.meta_uqer_dataDownLoad.get_span_list import (
    get_span_list,
)
from Euclid_work.Quant_Share.warehouse import load_data_from_sql
from Euclid_work.Quant_Share.TableInfo import tableInfo
from utils import update_h5_file, get_all_file
from datetime import date
from pathlib import Path

postgres_backup_path = Path(r"E:\Share\Stk_Data\postgres_backup")
# tableNameList = []
tableNameList = [
    "balance_sheet",
    "deriv_finance_indicator",
    "continuous_contracts",
    "gmData_history_adj",
    "gmData_history_1m",
    "gmData_history",
    "uqer_MktLimit",
    "uqer_MktIdx",
    "uqer_FdmtIndiRtnPit",
    "uqer_mkt_equd_adj",
]
for tableName in tableNameList:
    sub_path = postgres_backup_path / tableName
    sub_path.mkdir(parents=True, exist_ok=True)
    file_list, _ = get_all_file(sub_path)
    # TODO 太丑陋
    latest_update_date = "{}-{:0>2d}-01".format(
        file_list[-1].split("Y")[-1].split("_")[0],
        int(file_list[-1].split("M")[-1].split(".")[0]),
    )
    for begin_data, end_date, tag in get_span_list(
        latest_update_date, date.today(), freq="m"
    ):
        data = load_data_from_sql(
            tableName,
            begin=begin_data,
            end=end_date,
            ticker_column=tableInfo[tableName]["ticker_column"],
            date_column=tableInfo[tableName]["date_column"],
        )
        update_h5_file(
            data,
            full_path=sub_path / "{}_{}.h5".format(tableName, tag),
        )
