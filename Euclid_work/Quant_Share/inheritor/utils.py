from typing import Union
from collections import OrderedDict
from configparser import ConfigParser
import os
from typing import Sequence, Iterator
from .consts import Config, TimeType
import pandas as pd
from datetime import datetime, date
from time import strptime


__all__ = ["Formatter", "packaging", 'get_config']


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

        res.columns = res.columns.rename(format_stock)
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

def packaging(
    series: Sequence, pat: int, iterator: bool = False
) -> Sequence[Sequence] | Iterator:
    assert pat > 0
    if iterator:
        for i in range(0, len(series), pat):
            yield series[i : i + pat]
    else:
        return [series[i : i + pat] for i in range(0, len(series), pat)]




def get_config(
    filename: Union[str, os.PathLike] = "./quant.const.ini", section: str = None
) -> OrderedDict:
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
