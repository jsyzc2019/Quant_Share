# -*- coding: utf-8 -*-
# @Time    : 2023/4/8 18:47
# @Author  : Alkaid-Yuan
# @File    : chronos.py


from datetime import datetime, date
from time import strptime
from typing import Union, Optional, Literal
import numpy as np
import pandas as pd


def watcher(func):
    @wraps(func)
    def timer(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        print(f"“{func.__name__}” run time: {end - start}.")
        return result

    return timer


TimeType = Union[str, int, datetime, date, pd.Timestamp]


class TradeDate:
    dataBase_root_path = r"E:\Share\Stk_Data\dataFile"
    trade_date_table = pd.read_hdf("{}/tradeDate_info.h5".format(dataBase_root_path))
    trade_date_list = trade_date_table["tradeDate"].dropna().to_list()

    @classmethod
    def is_date(
        cls, date_repr: TimeType, pattern_return: bool = False, **kwargs
    ) -> bool | str:
        if not isinstance(date_repr, str):
            date_repr = str(date_repr)

        chinesenum = {
            "一": "1",
            "二": "2",
            "三": "3",
            "四": "4",
            "五": "5",
            "六": "6",
            "七": "7",
            "八": "8",
            "九": "9",
            "零": "0",
            "十": "10",
        }
        strdate = ""
        for i in range(len(date_repr)):
            temp = date_repr[i]
            if temp in chinesenum:
                if temp == "十":
                    if datestr[i + 1] not in chinesenum:
                        strdate += chinesenum[temp]
                    elif datestr[i - 1] in chinesenum:
                        continue
                    else:
                        strdate += "1"
                else:
                    strdate += chinesenum[temp]
            else:
                strdate += temp

        pattern = (
            "%Y年%m月%d日",
            "%Y-%m-%d",
            "%y年%m月%d日",
            "%y-%m-%d",
            "%Y/%m/%d",
            "%Y%m%d",
        ) + kwargs.get("pattern", ())
        for i in pattern:
            try:
                ret = strptime(strdate, i)
                if ret:
                    return True if not pattern_return else i
            except ValueError as _:
                continue
        return False if not pattern_return else None

    @classmethod
    def format_date(
        cls,
        date_repr: Union[TimeType, pd.Series, list, tuple],
        **kwargs,
    ) -> pd.Series | pd.Timestamp:

        if isinstance(date_repr, (list, tuple, pd.Series)):
            if isinstance(date_repr[0], (datetime, date)):
                return pd.to_datetime(date_repr)
            elif isinstance(date_repr[0], (int, str)):
                pattern = cls.is_date(date_repr[0], pattern_return=True, **kwargs)
                return pd.to_datetime(date_repr, format=pattern)
        elif isinstance(date_repr, TimeType):
            if isinstance(date_repr, (datetime, date, pd.Timestamp)):
                return pd.to_datetime(date_repr)
            elif isinstance(date_repr, (int, str)):
                pattern = cls.is_date(date_repr, pattern_return=True, **kwargs)
                return pd.to_datetime(date_repr, format=pattern)
        else:
            raise TypeError(f"date_repr {type(date_repr)} is not supported")

    @classmethod
    def extend_date_span(
        cls, begin: TimeType, end: TimeType, freq: Literal["Q", "q", "Y", "y", "M", "m"]
    ) -> tuple[datetime, datetime]:
        """
        将区间[begin, end] 进行拓宽, 依据freq将拓展至指定位置, 详见下
        freq = M :
            [2018-01-04, 2018-04-20] -> [2018-01-01, 2018-04-30]
            [2018-01-01, 2018-04-20] -> [2018-01-01, 2018-04-30]
            [2018-01-04, 2018-04-30] -> [2018-01-01, 2018-04-30]
        freq = Q :
            [2018-01-04, 2018-04-20] -> [2018-01-01, 2018-06-30]
            [2018-01-01, 2018-04-20] -> [2018-01-01, 2018-06-30]
            [2018-01-04, 2018-06-30] -> [2018-01-01, 2018-06-30]
        freq = Y :
            [2018-01-04, 2018-04-20] -> [2018-01-01, 2018-12-31]
            [2018-01-01, 2018-04-20] -> [2018-01-01, 2018-12-31]
            [2018-01-04, 2018-12-31] -> [2018-01-01, 2018-12-31]
        """
        begin = cls.format_date(begin)
        end = cls.format_date(end)
        if freq in ["Q", "q"]:
            if not pd.DateOffset().is_quarter_start(begin):
                begin = begin - pd.offsets.QuarterBegin(n=1, startingMonth=1)
            if not pd.offsets.DateOffset().is_quarter_end(end):
                end = end + pd.offsets.QuarterEnd(n=1)
            return begin, end

        elif freq in ["Y", "y"]:
            if not pd.offsets.DateOffset().is_year_start(begin):
                begin = begin - pd.offsets.YearBegin(n=1)
            if not pd.offsets.DateOffset().is_year_end(end):
                end = end + pd.offsets.YearEnd(n=1)
            return begin, end
        elif freq in ["M", "m"]:
            if not pd.DateOffset().is_month_start(begin):
                begin = begin - pd.offsets.MonthBegin(n=1)
            if not pd.offsets.DateOffset().is_month_end(end):
                end = end + pd.offsets.MonthEnd(n=1)
            return begin, end
        else:
            raise AttributeError("frq should be M, Q or Y!")

    @classmethod
    def is_trade_date(
            cls,
            date_repr: TimeType
    ) -> bool:
        # TODO：二分法优化
        return cls.format_date(date_repr) in cls.trade_date_list
