import os
from collections import OrderedDict
from configparser import ConfigParser
from datetime import datetime, date
from functools import wraps
from time import strptime
from typing import Sequence, Iterator
from typing import Union, Literal, Tuple, Any
from .consts import datatables
import numpy as np
import pandas as pd

TimeType = Union[str, int, datetime, date, pd.Timestamp]

__all__ = [
    "Formatter",
    "TradeDate",
    "Config",
    "Common",
]


class Config:
    database_dir = {
        "root": r"E:\Share\Stk_Data\dataFile",
        "future": r"E:\Share\Fut_Data",
        "gm_stock_factor": r"E:\Share\Stk_Data\gm",
        "em_stock_factor": r"E:\Share\EM_Data",
        "jq_stock_factor": r"E:\Share\JointQuant_Factor",
        "jq_data_prepare": r"E:\Share\JointQuant_prepare",
    }

    datasets_name = list(database_dir.keys())
    datatables = datatables

    stock_table: pd.DataFrame = pd.read_hdf(
        "{}/stock_info.h5".format(database_dir["root"])
    )
    stock_list = stock_table["symbol"].tolist()
    stock_num_list = stock_table["sec_id"].unique().tolist()

    futures_list: list[str | Any] = (
        "AG",
        "AL",
        "AU",
        "A",
        "BB",
        "BU",
        "B",
        "CF",
        "CS",
        "CU",
        "C",
        "FB",
        "FG",
        "HC",
        "IC",
        "IF",
        "IH",
        "I",
        "JD",
        "JM",
        "JR",
        "J",
        "LR",
        "L",
        "MA",
        "M",
        "NI",
        "OI",
        "PB",
        "PM",
        "PP",
        "P",
        "RB",
        "RI",
        "RM",
        "RS",
        "RU",
        "SF",
        "SM",
        "SN",
        "SR",
        "TA",
        "TF",
        "T",
        "V",
        "WH",
        "Y",
        "ZC",
        "ZN",
        "PG",
        "EB",
        "AP",
        "LU",
        "SA",
        "TS",
        "CY",
        "IM",
        "PF",
        "PK",
        "CJ",
        "UR",
        "NR",
        "SS",
        "FU",
        "EG",
        "LH",
        "SP",
        "RR",
        "SC",
        "WR",
        "BC",
    )

    trade_date_table: pd.DataFrame = pd.read_hdf(
        "{}/tradeDate_info.h5".format(database_dir["root"])
    )
    trade_date_list = trade_date_table["tradeDate"].dropna().to_list()

    quarter_begin = ["0101", "0401", "0701", "1001"]
    quarter_end = ["0331", "0630", "0930", "1231"]


class TradeDate:
    trade_date_table = Config.trade_date_table
    trade_date_list = Config.trade_date_list

    @classmethod
    def is_date(
        cls, date_repr: TimeType, pattern_return: bool = False, **kwargs
    ) -> bool | str:

        return Formatter.is_date(date_repr, pattern_return, **kwargs)

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
        """
        :param arr:
        :param target:
        :return:
        """
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
        """
        :param date_repr:
        :param lag:
        :return:
        """
        date_repr = cls.format_date(date_repr)
        res, index = cls.binary_search(cls.trade_date_list, date_repr)
        return cls.trade_date_list[index + lag]

    @classmethod
    def range_trade_date(cls, begin: TimeType, end: TimeType = None, lag: int = None):
        """
        :param begin:
        :param end:
        :param lag:
        :return:
        """
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


class Formatter:
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
        date_str_res = ""
        for i in range(len(date_repr)):
            temp = date_repr[i]
            if temp in chinesenum:
                if temp == "十":
                    if date_repr[i + 1] not in chinesenum:
                        date_str_res += chinesenum[temp]
                    elif date_repr[i - 1] in chinesenum:
                        continue
                    else:
                        date_str_res += "1"
                else:
                    date_str_res += chinesenum[temp]
            else:
                date_str_res += temp

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
                ret = strptime(date_str_res, i)
                if ret:
                    return True if not pattern_return else i
            except ValueError as _:
                continue
        return False if not pattern_return else None

    @classmethod
    def date(
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
    def dataframe(cls, data: pd.DataFrame, **kwargs):
        res = data.copy()
        if not isinstance(res, pd.DataFrame):
            raise TypeError("data must be a DataFrame")
        try:
            res.index = pd.to_datetime(res.index)
        except ValueError:
            try:
                res.index = list(map(TradeDate.format_date, res.index))
            except TypeError as te:
                raise te

        res = res.rename(columns=cls.stock)
        if np.nan in res.columns:
            res = res.drop(columns=np.nan)

        begin = kwargs.get("begin", res.index.min())
        end = kwargs.get("end", res.index.max())

        new_index = pd.date_range(begin, end, freq="D").intersection(
            TradeDate.trade_date_list
        )
        new_columns = list(map(cls.stock, Config.stock_list))

        fill_value = kwargs.get("fill", np.nan)
        return res.reindex(index=new_index, columns=new_columns, fill_value=fill_value)

    @classmethod
    def stock(cls, code: str | int) -> str:
        """
        Standardize the stock code to wind format
        :param code: '000001', '000001.XSHE' etc.
        :return: '000001.SZ'
        """
        if isinstance(code, str):
            if code[-2:] in ["BJ", "SZ", "SH"]:
                return code
            elif "." in code or code.isdigit():
                if code[:6].isdigit():
                    num = code[:6]
                elif code[-6:].isdigit():
                    num = code[-6:]
                else:
                    raise ValueError("Invalid stock code")
                code = int(num)
            else:
                return np.nan
        tag = code // 100000
        if tag in [4, 8]:
            tail = "BJ"
        elif code < 500000:
            tail = "SZ"
        else:
            tail = "SH"
        format_code = "{:06.0f}.{}".format(code, tail)
        return format_code


class Common:
    @classmethod
    def info_lag(cls, data: pd.DataFrame, n_lag: int, clean: bool = False):
        """
        Delay the time corresponding to the data by n trading days
        :param clean:
        :param data:
        :param n_lag:
        :return:
        """
        res = data.copy()
        res = res.sort_index()
        if clean:
            res = Formatter.dataframe(res)
        res = res.shift(n_lag)
        res = res.dropna(axis=0, how="all")
        return res

    @classmethod
    def watcher(cls, func: callable):
        """
        :param func:
        :return:
        """

        @wraps(func)
        def timer(*args, **kwargs):
            """
            :param args:
            :param kwargs:
            :return:
            """
            start = datetime.now()
            result = func(*args, **kwargs)
            end = datetime.now()
            print(f"“{func.__name__}” run time: {end - start}.")
            return result

        return timer

    @classmethod
    def packaging(
            cls, series: Sequence, pat: int, iterator: bool = False
    ) -> Sequence[Sequence] | Iterator:
        """
        :param series:
        :param pat:
        :param iterator:
        :return:
        """
        assert pat > 0
        if iterator:
            for i in range(0, len(series), pat):
                yield series[i : i + pat]
        else:
            return [series[i : i + pat] for i in range(0, len(series), pat)]

    @classmethod
    def get_config(
            cls,
            filename: Union[str, os.PathLike] = "./quant.const.ini",
            section: str = None,
    ) -> OrderedDict:
        """

        :param filename:
        :param section:
        :return:
        """
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)
        res = OrderedDict()
        if section is None:
            for sec in parser.sections():
                params = parser.items(sec)
                tmp = OrderedDict()
                for key, val in params:
                    tmp[key] = val
                res[sec] = tmp

        return res