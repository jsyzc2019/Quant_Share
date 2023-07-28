from .base_package import *
from .rolling_save import rolling_save


def ResConIndustrySwFy12(begin, end, **kwargs):
    """
    中信行业一致预期动态预测数据表
    :param begin:
    :param end:
    :param kwargs:
    :return:
    """
    if "indexID" not in kwargs.keys():
        raise AttributeError("indexID should in kwargs!")
    data = DataAPI.ResConIndustrySwFy12(
        ticker=[],
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
        indexID=kwargs.get("indexID")
    )
    return data


def ResConIndustrySwFy12_update(upDateBegin, endDate=None):
    endDate = endDate or date.today().strftime("%Y%m%d")
    rolling_save(
        ResConIndustrySwFy12,
        "ResConIndustrySwFy12",
        upDateBegin,
        endDate,
        freq="Q",
        subPath="{}/ResConIndustrySwFy12".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=[],
        indexID=3
    )
