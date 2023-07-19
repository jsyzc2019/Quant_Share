"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 16:41
# @Author  : Euclid-Jie
# @File    : rolling_save.py
"""
from .get_span_list import get_span_list
from .base_package import *


def rolling_save(func, tableName, begin, end, freq, subPath, **kwargs):
    """
    对每段时间进行数据获取, 随即写入, 适用于DataYes数据更新
    param:
        func: 更新数据所用的函数, data = func(begin, end, **kwargs)
        tableName: 数据存储的表名
        begin: 数据更新的开始时间
        end: 数据更新的结束时间
        freq: 数据分段读取的freq
        subPath: 存储数据的名字, 一般为: "{}/{}".format(dataBase_root_path, tableName)
    kwargs:
        reWrite: 写入数据时, 是否开启覆写, 默认为False
        monthlyStack: 获取数据时, 是都开启逐月获取, 默认为False
            部分表, 每次返回的数据量有限制, 需要细分区间, 获取subData后stack
        others: 会传入func中
    """
    spanList = get_span_list(begin, end, freq=freq)

    # reWrite or not
    reWrite = kwargs.get("reWrite", False)

    # monthly Stack or not
    monthlyStack = kwargs.get("monthlyStack", False)

    if monthlyStack and freq in ["Q", "q", "Y", "y"]:
        with tqdm(spanList) as t:
            for begin_day, end_day, tag in t:
                t.set_postfix({"span": "{}-{}".format(begin_day, end_day)})
                data = pd.DataFrame()
                for _begin, _end, __ in get_span_list(begin_day, end_day, freq="m"):
                    tmpData = func(
                        _begin.strftime("%Y%m%d"), _end.strftime("%Y%m%d"), **kwargs
                    )
                    data = pd.concat([data, tmpData], axis=0, ignore_index=True)
                save_data_h5(
                    data,
                    name="{}_{}".format(tableName, tag),
                    subPath=subPath,
                    reWrite=reWrite,
                )

    else:  # (monthlyStack=Ture and freq in ['M', 'm'])  or monthlyStack=False
        with tqdm(spanList) as t:
            for begin_day, end_day, tag in t:
                t.set_postfix({"span": "{}-{}".format(begin_day, end_day)})
                data = func(
                    begin_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d"), **kwargs
                )
                save_data_h5(
                    data,
                    name="{}_{}".format(tableName, tag),
                    subPath=subPath,
                    reWrite=reWrite,
                )
