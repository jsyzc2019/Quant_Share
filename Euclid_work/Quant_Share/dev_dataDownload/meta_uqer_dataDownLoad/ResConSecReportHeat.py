from .base_package import *
from .rolling_save import rolling_save
from Euclid_work.Quant_Share import stockNumList


def ResConSecReportHeat(begin, end, **kwargs):
    """
    个股研报热度统计数据表
    :param begin:
    :param end:
    :param kwargs:
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecReportHeat(
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


def ResConSecReportHeat_update(upDateBegin, endDate=None):
    today = date.today()
    endDate = endDate or today.strftime("%Y%m%d")
    secCode = stockNumList
    rolling_save(
        ResConSecReportHeat,
        "ResConSecReportHeat",
        upDateBegin,
        endDate,
        freq="Q",
        subPath="{}/ResConSecReportHeat".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        secCode=secCode
    )
