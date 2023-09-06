from .base_package import *
from .rolling_save import rolling_save


def ResConIndustryCitic(begin, end, **kwargs):
    """
    中信行业一致预期数据表
    :param begin:
    :param end:
    :param kwargs:
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.ResConIndustryCitic(
        ticker=kwargs["ticker"],
        secID="",
        beginDate=begin,
        endDate=end,
        beginYear="",
        endYear="",
        reportType="",
        publishDateEnd="",
        publishDateBegin="",
        field="",
        pandas="1",
        pat_len=100,
    )
    return data


def ResConIndustryCitic_update(upDateBegin, endDate=None):
    endDate = endDate or date.today().strftime("%Y%m%d")
    rolling_save(
        ResConIndustryCitic,
        "ResConIndustryCitic",
        upDateBegin,
        endDate,
        freq="Q",
        subPath="{}/ResConIndustryCitic".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=[],
    )
