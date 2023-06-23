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
    with open(os.path.join(current_dir, 'token.txt'), 'rt', encoding='utf-8') as f:
        token = f.read().strip()
    client = Client()
    client.init(token)

    @classmethod
    def MktEqudGet(cls, ticker: Union[list, str],
                   tradeDate: Union[list[Union[pd.datetime, str]], pd.datetime, str] = '',
                   beginDate: Union[pd.datetime, str, int] = None, endDate: Union[pd.datetime, str, int] = None,
                   **kwargs):
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
        beginDate, endDate, tradeDate = cls.assert_format_data(beginDate, endDate, tradeDate)
        base_url = '/api/market/getMktEqud.json?field=&beginDate={}&endDate={}&secID=&ticker={}&tradeDate={}'
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(base_url.format(",".join(pat_ticker_list),
                                                               beginDate,
                                                               endDate,
                                                               ",".join(tradeDate)
                                                               )
                                               )
                try:
                    outData_df = pd.concat([outData_df, pd.DataFrame(eval(result)["data"])])
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(",".join(ticker),
                                                           beginDate,
                                                           endDate,
                                                           ",".join(tradeDate)
                                                           )
                                           )
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def MktLimitGet(cls, ticker: Union[list, str],
                    tradeDate: Union[list[Union[pd.datetime, str]], pd.datetime, str] = '',
                    beginDate: Union[pd.datetime, str, int] = None, endDate: Union[pd.datetime, str, int] = None,
                    **kwargs):
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
        beginDate, endDate, tradeDate = cls.assert_format_data(beginDate, endDate, tradeDate)
        base_url = '/api/market/getMktLimit.json?field=&secID=&ticker={}&exchangeCD={}&tradeDate={}&beginDate={}&endDate={}'
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(base_url.format(",".join(pat_ticker_list),
                                                               kwargs.get('exchangeCD', ''),
                                                               ",".join(tradeDate),
                                                               beginDate,
                                                               endDate
                                                               )
                                               )
                try:
                    outData_df = pd.concat([outData_df, pd.DataFrame(eval(result)["data"])])
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(",".join(ticker),
                                                           kwargs.get('exchangeCD', ''),
                                                           ",".join(tradeDate),
                                                           beginDate,
                                                           endDate
                                                           )
                                           )
            return pd.DataFrame(eval(result)["data"])

    @staticmethod
    def assert_format_data(beginDate, endDate, tradeDate):
        if beginDate is None and endDate is None and tradeDate == '':
            raise AttributeError("begin + end or tradeDate should be param in")

        elif beginDate and endDate and tradeDate == '':
            beginDate = format_date(beginDate).strftime("%Y%m%d")
            endDate = format_date(endDate).strftime("%Y%m%d")
            tradeDate = []

        elif beginDate is None and endDate is None and tradeDate != '':
            beginDate = []
            endDate = []
            if isinstance(tradeDate, list):
                tradeDate = [format_date(tradeDate_i).strftime("%Y%m%d") for tradeDate_i in tradeDate]
            else:
                tradeDate = [format_date(tradeDate).strftime("%Y%m%d")]
        else:
            raise AttributeError("begin + end or tradeDate should be param in")
        return beginDate, endDate, tradeDate
