from .base_package import *
from .rolling_save import rolling_save
from Euclid_work.Quant_Share import stockNumList


def ResConSecCoredata(begin, end, **kwargs):
    """
    个股一致预期核心表
    :param begin:
    :param end:
    :param kwargs:
    :return:
    """
    if "secCode" not in kwargs.keys():
        raise AttributeError("secCode should in kwargs!")
    data = DataAPI.ResConSecCoredata(
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


def ResConSecCoredata_update(upDateBegin, endDate=None):
    today = date.today()
    endDate = endDate or today.strftime("%Y%m%d")
    secCode = stockNumList
    rolling_save(
        ResConSecCoredata,
        "ResConSecCoredata",
        upDateBegin,
        endDate,
        freq="Q",
        subPath="{}/ResConSecCoredata".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        secCode=secCode
    )
