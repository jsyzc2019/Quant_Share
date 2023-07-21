"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 12:54
# @Author  : Euclid-Jie
# @File    : FakeDataAPI.py
# @Desc    : 使用Uqer的WebAPI仿制本地SDK的DataAPI
"""
from typing import Union
import os
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from Euclid_work.Quant_Share.Utils import format_date, patList
from .dataapi_win36 import Client


class FakeDataAPI:
    # 通联登录
    current_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(current_dir, "token.txt"), "rt", encoding="utf-8") as f:
        token = f.read().strip()
    client = Client()
    client.init(token)

    @classmethod
    def MktEqudGet(
        cls,
        ticker: Union[list, str],
        tradeDate: Union[list[Union[pd.datetime, str]], pd.datetime, str] = "",
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/80
        demoUrl: /api/market/getMktEqud.json?field=&beginDate=&endDate=&secID=&ticker=688001&tradeDate=20190723
        :param ticker:
        :param tradeDate:
        :param beginDate:
        :param endDate:
        :param kwargs:
            pat_len: int
        :return:
        """
        beginDate, endDate, tradeDate = cls.assert_format_data(
            beginDate, endDate, tradeDate
        )
        base_url = "/api/market/getMktEqud.json?field=&beginDate={}&endDate={}&secID=&ticker={}&tradeDate={}"
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(
                    base_url.format(
                        beginDate,
                        endDate,
                        ",".join(pat_ticker_list),
                        ",".join(tradeDate),
                    )
                )
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(
                base_url.format(beginDate, endDate, ticker, ",".join(tradeDate))
            )
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def mIdxCloseWeightGet(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/1905
        demoUrl: /api/idx/getmIdxCloseWeight.json?field=&ticker=000300&secID=&beginDate=20151101&endDate=20151130
        :param ticker: default=[000300,000905,000852]
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/idx/getmIdxCloseWeight.json?field=&ticker={}&secID=&beginDate={}&endDate={}"
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(ticker):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(ticker, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def MktLimitGet(
        cls,
        ticker: Union[list, str],
        tradeDate: Union[list[Union[pd.datetime, str]], pd.datetime, str] = "",
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/1357
        demoUrl: https://mall.datayes.com/datapreview/1357
        :param ticker:
        :param tradeDate:
        :param beginDate:
        :param endDate:
        :param kwargs:
            exchangeCD: list[str],
            pat_len: int
        :return:
        """
        beginDate, endDate, tradeDate = cls.assert_format_data(
            beginDate, endDate, tradeDate
        )
        base_url = "/api/market/getMktLimit.json?field=&secID=&ticker={}&exchangeCD={}&tradeDate={}&beginDate={" \
                   "}&endDate={}"
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(
                    base_url.format(
                        ",".join(pat_ticker_list),
                        ",".join(kwargs.get("exchangeCD", "")),
                        ",".join(tradeDate),
                        beginDate,
                        endDate,
                    )
                )
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(
                base_url.format(
                    ticker,
                    ",".join(kwargs.get("exchangeCD", "")),
                    ",".join(tradeDate),
                    beginDate,
                    endDate,
                )
            )
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def MktIdxdGet(
        cls,
        indexID: Union[list, str],
        tradeDate: Union[list[Union[pd.datetime, str]], pd.datetime, str] = "",
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        无权限
        """
        beginDate, endDate, tradeDate = cls.assert_format_data(beginDate, endDate, "")
        base_url = "/api/market/getMktIdxd.json?field=&indexID={}&ticker=&exchangeCD={}&tradeDate={}&beginDate={}&endDate={}"
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(indexID, list):
            outData_df = pd.DataFrame()
            for pat_indexID_list in tqdm(patList(indexID, pat_len)):
                _, result = cls.client.getData(
                    base_url.format(
                        ",".join(pat_indexID_list),
                        ",".join(kwargs.get("exchangeCD", "")),
                        ",".join(tradeDate),
                        beginDate,
                        endDate,
                    )
                )
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(
                base_url.format(
                    indexID,
                    ",".join(kwargs.get("exchangeCD", "")),
                    ",".join(tradeDate),
                    beginDate,
                    endDate,
                )
            )
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def FdmtIndiRtnPitGet(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/1853
        demoUrl: https://mall.datayes.com/datapreview/1853
        :param ticker:
        :param beginDate:
        :param endDate:
        :param kwargs:
            pat_len: int, 5
            reportType: list[str], ["Q1", "S1", "CQ3", "A"]
            endYear: str, "2019"
            endYear: str, "2020"
        :return:
        """
        # TODO 似乎beginDate和endDate并不起filter作用
        beginDate, endDate, tradeDate = cls.assert_format_data(beginDate, endDate, "")
        base_url = "/api/fundamental/getFdmtIndiRtnPit.json?field=&ticker={}&beginYear={}&endYear={}&reportType={}&publishDateBegin{}=&publishDateEnd={}"
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(
                    base_url.format(
                        ",".join(pat_ticker_list),
                        kwargs.get("beginYear", ""),
                        kwargs.get("endYear", ""),
                        ",".join(kwargs.get("reportType", [])),
                        beginDate,
                        endDate,
                    )
                )
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(
                base_url.format(
                    ticker,
                    kwargs.get("beginYear", ""),
                    kwargs.get("endYear", ""),
                    ",".join(kwargs.get("reportType", [])),
                    beginDate,
                    endDate,
                )
            )
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def FdmtIndiPSPitGet(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/1851
        demoUrl = /api/fundamental/getFdmtIndiPSPit.json?field=&ticker=688002&beginYear=&endYear=&reportType=&publishDateBegin=&publishDateEnd=
        :param ticker:
        :param beginDate:
        :param endDate:
        :param kwargs:
            pat_len: int, 5
            reportType: list[str], ["Q1", "S1", "CQ3", "A"]
            beginYear: str, 2019
            endYear: str, 2020
        :return:
        """
        beginDate, endDate, tradeDate = cls.assert_format_data(beginDate, endDate, "")
        base_url = "/api/fundamental/getFdmtIndiPSPit.json?field=&ticker={}&beginYear={}&endYear={}&reportType={" \
                   "}&publishDateBegin={}&publishDateEnd={}"
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(
                    base_url.format(
                        ",".join(pat_ticker_list),
                        kwargs.get("beginYear", ""),
                        kwargs.get("endYear", ""),
                        ",".join(kwargs.get("reportType", [])),
                        beginDate,
                        endDate,
                    )
                )
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(
                base_url.format(
                    ticker,
                    kwargs.get("beginYear", ""),
                    kwargs.get("endYear", ""),
                    ",".join(kwargs.get("reportType", [])),
                    beginDate,
                    endDate,
                )
            )
            return pd.DataFrame(eval(result)["data"])

    @staticmethod
    def assert_format_data(beginDate, endDate, tradeDate=""):
        if beginDate is None and endDate is None and tradeDate == "":
            raise AttributeError("begin + end or tradeDate should be param in")

        elif beginDate and endDate and tradeDate == "":
            beginDate = format_date(beginDate).strftime("%Y%m%d")
            endDate = format_date(endDate).strftime("%Y%m%d")
            tradeDate = []

        elif beginDate is None and endDate is None and tradeDate != "":
            beginDate = []
            endDate = []
            if isinstance(tradeDate, list):
                tradeDate = [
                    format_date(tradeDate_i).strftime("%Y%m%d")
                    for tradeDate_i in tradeDate
                ]
            else:
                tradeDate = [format_date(tradeDate).strftime("%Y%m%d")]
        else:
            raise AttributeError("begin + end or tradeDate should be param in")
        return beginDate, endDate, tradeDate
