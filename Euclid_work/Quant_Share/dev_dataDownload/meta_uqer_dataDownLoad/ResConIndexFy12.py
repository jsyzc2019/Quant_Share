from .base_package import *
from .rolling_save import rolling_save


def ResConIndexFy12(begin, end, **kwargs):
    """
    指数一致预期动态预测数据表
    :param begin:
    :param end:
    :param kwargs:
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.ResConIndexFy12(
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


def ResConIndexFy12_update(upDateBegin, endDate=None):
    endDate = endDate or date.today().strftime("%Y%m%d")
    rolling_save(
        ResConIndexFy12,
        "ResConIndexFy12",
        upDateBegin,
        endDate,
        freq="Y",
        subPath="{}/ResConIndexFy12".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=["000300", "000905", "000852"],
    )
