from typing import Union
from collections import OrderedDict
from configparser import ConfigParser
import os
from typing import Sequence, Iterator


def packaging(
        series: Sequence, pat: int, iterator: bool = False
) -> Sequence[Sequence] | Iterator:
    assert pat > 0
    if iterator:
        for i in range(0, len(series), pat):
            yield series[i: i + pat]
    else:
        return [series[i: i + pat] for i in range(0, len(series), pat)]


def format_stock(code: str | int) -> str:
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
