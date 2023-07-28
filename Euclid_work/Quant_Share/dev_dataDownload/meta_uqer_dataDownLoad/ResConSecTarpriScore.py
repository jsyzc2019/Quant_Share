from .base_package import *
from .rolling_save import rolling_save
from Euclid_work.Quant_Share import stockNumList


def ResConSecTarpriScore(begin, end, **kwargs):
    """
    个股一致预期目标价与评级表
    :param begin:
    :param end:
    :param kwargs:
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecTarpriScore(
        secCode=kwargs.get("secCode"),
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
        pat_len=100
    )
    return data


def ResConSecTarpriScore_update(upDateBegin, endDate=None):
    today = date.today()
    endDate = endDate or today.strftime("%Y%m%d")
    secCode = stockNumList
    rolling_save(
        ResConSecTarpriScore,
        "ResConSecTarpriScore",
        upDateBegin,
        endDate,
        freq="Y",
        subPath="{}/ResConSecTarpriScore".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        secCode=secCode
    )
