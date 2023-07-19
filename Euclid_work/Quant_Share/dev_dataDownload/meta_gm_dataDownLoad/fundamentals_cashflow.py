"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/14 20:42
# @Author  : Euclid-Jie
# @File    : fundamentals_cashflow.py
"""
from .base_package import *


def fundamentals_cashflow(begin, end, **kwargs):
    if "symbol" not in kwargs.keys():
        raise AttributeError("symbol should in kwargs!")

    if "fundamentals_cashflow_fields" not in kwargs.keys():
        raise AttributeError("fundamentals cashflow fields should in kwargs!")

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")

    # arg prepare
    symbol = kwargs["symbol"]
    fundamentals_cashflow_fields = kwargs["fundamentals_cashflow_fields"]

    outData = pd.DataFrame()
    update_exit = 0
    with tqdm(symbol) as t:
        t.set_description(("begin:{} -- end:{}".format(begin, end)))
        for symbol_i in t:
            tmpData = stk_get_fundamentals_cashflow(
                symbol=symbol_i,
                rpt_type=None,
                data_type=None,
                start_date=begin,
                end_date=end,
                fields=fundamentals_cashflow_fields,
                df=True,
            )
            _len = len(tmpData)
            t.set_postfix({"状态": "已成功获取{}条数据".format(_len)})
            if _len > 0:
                update_exit = 0
                outData = pd.concat([outData, tmpData], ignore_index=True)
            else:
                update_exit += 1
            if update_exit >= update_exit_limit:
                print("no data return, exit update")
                break
    return outData


def fundamentals_cashflow_update(upDateBegin, endDate="20231231"):
    fundamentals_cashflow_info = pd.read_excel(
        os.path.join(dev_files_dir, "fundamentals_cashflow_info.xlsx")
    )
    fundamentals_cashflow_fields = ",".join(
        fundamentals_cashflow_info["字段名"].to_list()[:-3]
    )  # 最后三个字段没有
    data = fundamentals_cashflow(
        begin=upDateBegin,
        end=endDate,
        symbol=symbolList,
        fundamentals_cashflow_fields=fundamentals_cashflow_fields,
    )
    if len(data) == 0:
        print("无数据更新")
    else:
        save_data_Y(
            data,
            "pub_date",
            "fundamentals_cashflow",
            reWrite=True,
            _dataBase_root_path=dataBase_root_path_gmStockFactor,
        )
