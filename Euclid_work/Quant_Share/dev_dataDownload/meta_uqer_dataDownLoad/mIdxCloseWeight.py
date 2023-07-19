from .base_package import *
from .rolling_save import rolling_save


def mIdxCloseWeight(begin, end, **kwargs):
    """
    财务指标—盈利能力 (Point in time)
    :param begin:
    :param end:
    :param kwargs: ticker = stockNumList
    :return:
    """
    if "ticker" not in kwargs.keys():
        raise AttributeError("ticker should in kwargs!")
    data = DataAPI.mIdxCloseWeightGet(
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
    )
    return data


def mIdxCloseWeight_update(upDateBegin, endDate=None):
    endDate = endDate or date.today().strftime("%Y%m%d")
    rolling_save(
        mIdxCloseWeight,
        "mIdxCloseWeight",
        upDateBegin,
        endDate,
        freq="Y",
        subPath="{}/mIdxCloseWeight".format(dataBase_root_path),
        reWrite=True,
        monthlyStack=False,
        ticker=["000300", "000905", "000852"],
    )
