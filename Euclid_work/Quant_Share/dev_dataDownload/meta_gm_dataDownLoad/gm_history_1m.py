"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/4 9:47
# @Author  : Euclid-Jie
# @File    : gm_history_1m.py
# @Desc    : 用于下载/更新60s bar数据数据至数据库
            - 更新适用于短日期间隔
            - 长间隔使用gmData_history_1m_download.py
"""
from .base_package import *


def gmData_history_1m(begin, end, **kwargs):
    if "symbol" not in kwargs.keys():
        raise AttributeError("symbol should in kwargs!")

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")
    errors_num = 0
    update_exit = 0

    outData = pd.DataFrame()
    with tqdm(kwargs["symbol"]) as t:
        for symbol_i in t:
            try:
                data = history(
                    symbol_i, frequency="60s", start_time=begin, end_time=end, df=True
                )
                _len = len(data)
                t.set_postfix({"状态": "已写入{}的{}条数据".format(symbol_i, _len)})  # 进度条右边显示信息
                errors_num = 0

                if _len > 0:
                    update_exit = 0
                    outData = pd.concat([outData, data], axis=0, ignore_index=True)
                else:
                    update_exit += 1
                if update_exit >= update_exit_limit:
                    t.set_postfix(
                        {"状态": "{} no data return, exit update".format(symbol_i)}
                    )
                    continue

            except GmError:
                errors_num += 1
                if errors_num > 5:
                    raise RuntimeError("重试五次后，仍旧GmError")
                time.sleep(60)
                t.set_postfix({"状态": "GmError:{}, 将第{}次重试".format(GmError, errors_num)})
    return outData


def gmData_history_1m_update(upDateBegin, endDate="20231231", **kwargs):
    assert kwargs.get("engine") is not None
    data = gmData_history_1m(begin=upDateBegin, end=endDate, symbol=symbolList)
    if len(data) == 0:
        print("无数据更新")
    else:
        write_df_to_pgDB(
            data, engine=kwargs.get("engine"), table_name="gmData_history_1m"
        )
