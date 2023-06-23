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
        :return:
        """
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

        base_url = '/api/market/getMktEqud.json?field=&beginDate={}&endDate={}&secID=&ticker={}&tradeDate={}'
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(cls.fill_uqer_url(base_url, pat_ticker_list, beginDate, endDate, tradeDate))
                try:
                    outData_df = pd.concat([outData_df, pd.DataFrame(eval(result)["data"])])
                except KeyError:
                    print(eval(result)["retMsg"])
                    break
            return outData_df
        else:
            _, result = cls.client.getData(cls.fill_uqer_url(base_url, [ticker], beginDate, endDate, tradeDate))
            return pd.DataFrame(eval(result)["data"])

    @staticmethod
    def fill_uqer_url(base_url, _ticker_list: list[str],
                      begin: str = None, end: str = None,
                      _tradeDate: list[str] = None):

        return base_url.format(begin, end, ",".join(_ticker_list), ",".join(_tradeDate))

    @staticmethod
    def run_thread_pool_sub(target, args, max_work_count):
        with ThreadPoolExecutor(max_workers=max_work_count) as t:
            res = [t.submit(target, i) for i in args]
            return res

    def load_file(self, Url_list):
        load_data = pd.DataFrame()
        res = self.run_thread_pool_sub(self.client.getData, Url_list, max_work_count=20)
        for future in as_completed(res):
            (code, res) = future.result()
            res = pd.DataFrame(eval(res)["data"])
            if len(res) > 0:
                # 拼接数据
                if isinstance(res.index[0], pd.datetime):
                    res = res.reset_index()
                load_data = pd.concat((load_data, res), axis=0, ignore_index=True)
        return load_data
