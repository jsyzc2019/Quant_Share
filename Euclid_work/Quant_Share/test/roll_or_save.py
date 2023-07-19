"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/24 11:16
# @Author  : Euclid-Jie
# @File    : roll_or_save.py
"""
from Euclid_work.Quant_Share import (
    bench_info,
    save_data_Y,
    dataBase_root_path_gmStockFactor,
)
from Euclid_work.Quant_Share.dev_dataDownload.meta_gm_dataDownLoad import (
    gmData_bench_price,
)
from Euclid_work.Quant_Share.dev_dataDownload.meta_uqer_dataDownLoad.rolling_save import (
    rolling_save,
)


def gmData_bench_price_save_test(upDateBegin, endDate="20231231"):
    bench_symbol_list = list(set(bench_info["symbol"]))[0:100]
    data = gmData_bench_price(begin=upDateBegin, end=endDate, symbol=bench_symbol_list)
    save_data_Y(
        data,
        "trade_date",
        "gmData_bench_price_save",
        reWrite=True,
        _dataBase_root_path=dataBase_root_path_gmStockFactor,
    )


def gmData_bench_price_roll_test(upDateBegin, endDate="20231231"):
    bench_symbol_list = list(set(bench_info["symbol"]))[0:100]
    rolling_save(
        gmData_bench_price,
        "gmData_bench_price_roll",
        upDateBegin,
        endDate,
        freq="y",
        subPath="{}/gmData_bench_price_roll".format(dataBase_root_path_gmStockFactor),
        reWrite=True,
        monthlyStack=False,
        symbol=bench_symbol_list,
    )


if __name__ == "__main__":
    gmData_bench_price_save_test(upDateBegin="20200101")
    gmData_bench_price_roll_test(upDateBegin="20200101")
