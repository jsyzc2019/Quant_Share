# -*- coding: utf-8 -*-
# @Time    : 2023/7/22
# @Author  : Alkaid-Yuan
# @File    : chronos.py


from datetime import datetime, date
from time import strptime
from typing import Union, Optional, Literal, Tuple
import numpy as np
import pandas as pd
from .consts import Config, TimeType
from .utils import Formatter

__all__ = ["watcher", "TradeDate"]


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

        return Formatter.is_date(date_repr, **kwargs)

    @classmethod
    def format_date(
        cls,
        date_repr: Union[TimeType, pd.Series, list, tuple],
        **kwargs,
    ) -> pd.Series | pd.Timestamp:

        return Formatter.date(date_repr, **kwargs)

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
        cls, arr: Union[pd.Series, list, tuple, np.ndarray], target: TimeType
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
            return cls.trade_date_list[index_begin : index_end + 1]
        elif lag is not None:
            if lag < 0:
                index_end = index_begin
                index_begin -= lag
            else:
                index_end = index_begin + lag
            return cls.trade_date_list[index_begin : index_end + 1]
        else:
            raise AttributeError("Pass attribute end or lag to the function!")
