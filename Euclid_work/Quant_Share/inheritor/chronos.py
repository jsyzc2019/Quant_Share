# -*- coding: utf-8 -*-
# @Time    : 2023/7/22
# @Author  : Alkaid-Yuan
# @File    : chronos.py


from datetime import datetime, date
from time import strptime
from typing import Union, Optional, Literal, Tuple
import numpy as np
import pandas as pd
from .consts import Config

TimeType = Union[str, int, datetime, date, pd.Timestamp]


def watcher(func):
    @wraps(func)
    def timer(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        print(f"“{func.__name__}” run time: {end - start}.")
        return result

    return timer


class TradeDate:

    trade_date_table = Config.trade_date_table
    trade_date_list = Config.trade_date_list

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
    def is_trade_date(cls, date_repr: TimeType) -> bool:
        res, _ = cls.binary_search(cls.trade_date_list, date_repr)
        return res

    @classmethod
    def binary_search(
        cls,
        arr: Union[pd.Series, list, tuple, np.ndarray],
        target: TimeType
    ) -> Tuple[bool, int]:
        if isinstance(arr[0], (pd.Timestamp, datetime, date)):
            target = cls.format_date(target)

        low: int = 0
        high: int = len(arr) - 1
        while low <= high:
            mid = (low + high) // 2
            if arr[mid] == target:
                return True, mid
            elif arr[mid] < target:
                low = mid + 1
            else:
                high = mid - 1
        return False, low

    @classmethod
    def shift_trade_date(
        cls,
        date_repr: TimeType,
        lag: int,
    ) -> pd.Timestamp:

        date_repr = cls.format_date(date_repr)
        res, index = cls.binary_search(cls.trade_date_list, date_repr)
        return cls.trade_date_list[index + lag]

    @classmethod
    def range_trade_date(cls, begin: TimeType, end: TimeType = None, lag: int = None):
        begin = cls.format_date(begin)
        _, index_begin = cls.binary_search(cls.trade_date_list, begin)
        if end is not None:
            end = cls.format_date(end)
            res, index_end = cls.binary_search(cls.trade_date_list, end)
            if not res:
                index_end += 1
            return cls.trade_date_list[index_begin:index_end+1]
        elif lag is not None:
            if lag < 0:
                index_end = index_begin
                index_begin -= lag
            else:
                index_end = index_begin + lag
            return cls.trade_date_list[index_begin:index_end + 1]
        else:
            raise AttributeError("Pass attribute end or lag to the function!")



