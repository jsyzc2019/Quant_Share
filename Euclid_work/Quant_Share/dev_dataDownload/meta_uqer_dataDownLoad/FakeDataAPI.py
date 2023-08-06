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
from ...Utils import format_date, patList
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
    def MktEqudAdjGet(
            cls,
            ticker: Union[list, str],
            tradeDate: Union[list[Union[pd.datetime, str]], pd.datetime, str] = "",
            beginDate: Union[pd.datetime, str, int] = None,
            endDate: Union[pd.datetime, str, int] = None,
            **kwargs
    ):
        """
        沪深股票前复权行情
        doc: https://mall.datayes.com/datapreview/1290
        demoUrl: /api/market/getMktEqudAdj.json?field=&secID=&ticker=688001&beginDate=&endDate=&tradeDate=20190816
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
        base_url = "/api/market/getMktEqudAdj.json?field=&beginDate={}&endDate={}&secID=&ticker={}&tradeDate={}"
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
        base_url = (
            "/api/market/getMktLimit.json?field=&secID=&ticker={}&exchangeCD={}&tradeDate={}&beginDate={"
            "}&endDate={}"
        )
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
    def ResConIndex(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/3686
        demoUrl: /api/researchReport/getResConIndex.json?field=&indexID=&indexCode=000002&beginDate=20211001&endDate=20211031
        :param ticker:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConIndex.json?field=&indexID=&indexCode={}&beginDate={}&endDate={}"
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
    def ResConIndexFy12(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        doc: https://mall.datayes.com/datapreview/3687
        demoUrl: /api/researchReport/getResConIndexFy12.json?field=&indexID=&indexCode=000016&beginDate=20211001&endDate=20211031
        :param ticker:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConIndexFy12.json?field=&indexID=&indexCode={}&beginDate={}&endDate={}"
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
    def ResConIndustryCitic(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        中信行业一致预期数据表
        doc: https://mall.datayes.com/datapreview/3688
        demoUrl: /api/researchReport/getResConIndustryCitic.json?field=&induID=&induLevel=&indexID=3&indexCode=&beginDate=20211015&endDate=20211015
        :param ticker:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConIndustryCitic.json?field=&induID=&induLevel=&indexID=3&indexCode=&beginDate={}&endDate={}"
        outData_df = pd.DataFrame()
        _, result = cls.client.getData(base_url.format(beginDate, endDate))
        try:
            outData_df = pd.concat([outData_df, pd.DataFrame(eval(result)["data"])])
        except KeyError:
            print(eval(result)["retMsg"])
        return outData_df

    @classmethod
    def ResConIndustryCiticFy12(
        cls,
        indexID: Union[list, int],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        中信行业一致预期动态预测数据表
        doc: https://mall.datayes.com/datapreview/3689
        demoUrl: /api/researchReport/getResConIndustryCiticFy12.json?field=&induID=&induLevel=&indexID=3&indexCode=&beginDate=20211015&endDate=20211015
        :param indexID:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConIndustryCiticFy12.json?field=&induID=&induLevel=&indexID={}&indexCode=&beginDate={}&endDate={}"
        if isinstance(indexID, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(indexID):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(indexID, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def ResConIndustrySw(
        cls,
        indexID: Union[int, list[int]] = 3,
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        申万行业一致预期数据表
        doc: https://mall.datayes.com/datapreview/3690
        demoUrl: /api/researchReport/getResConIndustrySw.json?field=&induID=&induID=&induLevel=&indexID=3&indexCode=&beginDate=20211015&endDate=20211015&repForeDate=
        :param indexID:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConIndustrySw.json?field=&induID=&induID=&induLevel=&indexID={}&indexCode=&beginDate={}&endDate={}&repForeDate="
        if isinstance(indexID, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(indexID):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(indexID, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def ResConIndustrySwFy12(
        cls,
        indexID: Union[int, list[int]] = 3,
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        申万行业一致预期动态预测数据表
        doc: https://mall.datayes.com/datapreview/3691
        demoUrl: /api/researchReport/getResConIndustrySwFy12.json?field=&induID=&induID=&induLevel=&indexID={}&indexCode=&beginDate={}&endDate={}&repForeDate=
        :param indexID:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConIndustrySwFy12.json?field=&induID=&induID=&induLevel=&indexID={}&indexCode=&beginDate={}&endDate={}&repForeDate="
        if isinstance(indexID, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(indexID):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(indexID, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def ResConSecReportHeat(
        cls,
        secCode,
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        个股研报热度统计数据表
        doc: https://mall.datayes.com/datapreview/3692
        demoUrl: /api/researchReport/getResConSecReportHeat.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate=
        :param secCode:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConSecReportHeat.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate="
        if isinstance(secCode, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(secCode):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(secCode, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def ResConSecCoredata(
        cls,
        secCode,
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        个股一致预期核心表
        doc: https://mall.datayes.com/datapreview/3692
        demoUrl: /api/researchReport/getResConSecCoredata.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate=
        :param secCode:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConSecCoredata.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate="
        if isinstance(secCode, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(secCode):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    # print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(secCode, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def ResConSecTarpriScore(
        cls,
        secCode,
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        个股一致预期目标价与评级表
        doc: https://mall.datayes.com/datapreview/3651
        demoUrl: /api/researchReport/getResConSecTarpriScore.json?field=&secCode=300896&secName=&beginDate=20210701&endDate=20210731&repForeDate=
        :param secCode:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConSecTarpriScore.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate="
        if isinstance(secCode, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(secCode):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    # print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(secCode, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def ResConSecCorederi(
        cls,
        secCode,
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        个股一致预期核心加工表
        doc: https://mall.datayes.com/datapreview/3653
        demoUrl: /api/researchReport/getResConSecCorederi.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate=
        :param secCode:
        :param beginDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        beginDate, endDate, _ = cls.assert_format_data(beginDate, endDate)
        base_url = "/api/researchReport/getResConSecCorederi.json?field=&secCode={}&secName=&beginDate={}&endDate={}&repForeDate="
        if isinstance(secCode, list):
            outData_df = pd.DataFrame()
            for ti in tqdm(secCode):
                _, result = cls.client.getData(base_url.format(ti, beginDate, endDate))
                try:
                    outData_df = pd.concat(
                        [outData_df, pd.DataFrame(eval(result)["data"])]
                    )
                except KeyError:
                    # print(eval(result)["retMsg"])
                    continue
            return outData_df
        else:
            _, result = cls.client.getData(base_url.format(secCode, beginDate, endDate))
            return pd.DataFrame(eval(result)["data"])

    @classmethod
    def MktAdjfGet(
        cls,
        ticker: Union[list, str],
        beginDate: Union[pd.datetime, str, int] = None,
        endDate: Union[pd.datetime, str, int] = None,
        **kwargs
    ):
        """
        前复权因子
        doc: https://mall.datayes.com/datapreview/1291
        demoUrl: /api/market/getMktAdjf.json?field=&secID=&ticker=000001&exDivDate=&beginDate=&endDate=20151123
        :param ticker:
        :param beginDate:
        :param endDate:
        :param kwargs:
            pat_len: int, 5
            exDivDate: str, "20200101"
        :return:
        """
        beginDate, endDate, tradeDate = cls.assert_format_data(beginDate, endDate, "")
        base_url = "/api/market/getMktAdjf.json?field=&secID=&ticker={}&exDivDate={}&beginDate={}&endDate={}"
        pat_len = kwargs.get("pat_len", 5)
        if isinstance(ticker, list):
            outData_df = pd.DataFrame()
            for pat_ticker_list in tqdm(patList(ticker, pat_len)):
                _, result = cls.client.getData(
                    base_url.format(
                        ",".join(pat_ticker_list),
                        kwargs.get("exDivDate", ""),
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
                    kwargs.get("exDivDate", ""),
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
        base_url = (
            "/api/fundamental/getFdmtIndiPSPit.json?field=&ticker={}&beginYear={}&endYear={}&reportType={"
            "}&publishDateBegin={}&publishDateEnd={}"
        )
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
