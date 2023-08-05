"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/26 19:14
# @Author  : Euclid-Jie
# @File    : signal_back_test.py
# @Desc    : 回测相关内容
"""
import os
from datetime import date, datetime
from typing import Union, Dict, List
import shutil

import pandas as pd
from bokeh.io import save  # 引入保存函数
from bokeh.models import WheelZoomTool, RangeTool, DatetimeTickFormatter
from bokeh.plotting import figure, output_file

from .signal_utils import *
from ..EuclidGetData import get_data
from ..H5DataSet import H5DataSet
from ..Utils import data2score, info_lag
from ..Utils import (
    format_date,
    tradeDate_info,
    format_stockCode,
    reindex,
)
from ..warehouse import load_gmData_history, postgres_engine

TimeType = Union[str, int, datetime, date, pd.Timestamp]


class QScache:
    def __init__(self, cache_path):
        self.cache_path = cache_path
        self.cache_dict = self.load_cache(cache_path)

    @classmethod
    def load_cache(cls, path):
        if isinstance(path, str):
            path = Path(path)
        cache = {}
        for item in path.iterdir():
            if item.is_dir():
                cache[item.name] = cls.load_cache(item)
            elif item.is_file() and item.name.endswith(".h5"):
                data: H5DataSet = H5DataSet(item)
                cache[item.name.split(".")[0]] = data
        return cache


class DataPrepare:
    def __init__(
        self,
        begin_date: TimeType = "2015-01-01",
        end_date: TimeType = None,
        data_path: str = "./",
        sub_name: str = "",
        **kwargs
    ):
        # format param
        self.begin_date = format_date(begin_date)
        end_date = date.today() if end_date is None else end_date
        self.end_date = format_date(end_date)

        # cache path
        self.back_test_data_path = Path(
            data_path,
            self.begin_date.strftime("%Y%m%d") + "_" + self.end_date.strftime("%Y%m%d"),
            sub_name,
        )
        Path.mkdir(self.back_test_data_path, exist_ok=True)
        print("cache path : {}".format(self.back_test_data_path.resolve()))

        # meta data: index(trade dates) and columns(stock ids)
        # # trade dates
        self.tradeDateBase = tradeDate_info.loc[
            format_date(self.begin_date) : format_date(self.end_date)
        ]
        self.trade_dates = np.unique(self.tradeDateBase["tradeDate"].dropna())
        ## stock ids
        # bench code
        bench_code = kwargs.get("bench_code", None)
        if bench_code is None:
            self.bench_code = "A_all"
            symbolList = pd.read_sql(
                """select symbol  from stock_info where delisted_date >= '2015-01-01'""",
                con=postgres_engine(),
            )["symbol"].values
            self.stock_ids = [format_stockCode(x) for x in symbolList]
        else:
            self.bench_code = str(bench_code).zfill(6)
            self.get_bench_info()
            # 由con code获取stock_ids
            self.stock_ids = np.unique(
                H5DataSet(
                    self.back_test_data_path.joinpath(
                        "bench_price", self.bench_code, "bench_con_code.h5"
                    )
                )
                .load_pivotDF_from_h5data(
                    pivotKey="bench_con_code"
                )  # 读取为data_frame, 随即取columns
                .columns
            )

        # price data
        self.get_price_data()

    @classmethod
    def load_cache(cls, path):
        if isinstance(path, str):
            path = Path(path)
        cache = {}
        for item in path.iterdir():
            if item.is_dir():
                cache[item.name] = cls.load_cache(item)
            elif item.is_file() and item.name.endswith(".h5"):
                data: H5DataSet = H5DataSet(item)
                cache[item.name.split(".")[0]] = data
        return cache

    @classmethod
    def calc_score(cls, data: pd.DataFrame):
        # calc total value rank score
        Score = data2score(data)
        # info lag, do not use future info
        return info_lag(Score, n_lag=1)

    def get_bench_info(self):
        # bench nav
        sub_path = self.back_test_data_path.joinpath("bench_price", self.bench_code)
        if sub_path.exists():
            print("bench_price has cache, pass")
            # load cache in back test
        else:
            sub_path.mkdir(parents=True)
            bench_price_df = pd.read_sql(
                """select tradedate,chgpct from  "uqer_MktIdx" where ticker = '{}'
                """.format(
                    self.bench_code
                ),
                con=postgres_engine(),
            )
            bench_price_df["tradedate"] = pd.to_datetime(bench_price_df["tradedate"])
            bench_price_df = bench_price_df.set_index("tradedate")
            bench_pct_chg = bench_price_df["chgpct"].reindex(
                self.trade_dates, fill_value=np.NaN
            )
            bench_pct_chg.to_hdf(sub_path.joinpath("bench_price_pct_chg.h5"), key="a")
            # bench con
            bench_con_code = (
                pd.read_sql(
                    """select constickersymbol, effDate, weight from uqer_midx_close_weight where ticker = '{}' order by effdate
                """.format(
                        self.bench_code
                    ),
                    con=postgres_engine(),
                )
                .drop_duplicates(subset=["constickersymbol", "effDate"])
                .pivot(index="effdate", columns="constickersymbol", values="weight")
            )
            bench_con_code.columns = [
                format_stockCode(i) for i in bench_con_code.columns
            ]
            bench_con_code = bench_con_code.reindex(
                self.trade_dates, fill_value=np.NaN
            ).fillna(method="ffill")
            H5DataSet.write_pivotDF_to_h5data(
                h5FilePath=sub_path.joinpath("bench_con_code.h5"),
                pivotDF=bench_con_code,
                pivotKey="bench_con_code",
                rewrite=True,
            )
            del bench_price_df, bench_con_code, bench_pct_chg

    def get_price_data(self):
        sub_path = self.back_test_data_path.joinpath("price_data", self.bench_code)
        if sub_path.exists():
            print("price_data has cache, pass")
            # H5DataSet(sub_path / "stock_price.h5").h5dir()
            # load cache
        else:
            sub_path.mkdir(parents=True)
            price_df = load_gmData_history(
                begin=self.begin_date, end=self.end_date, adj=True
            )
            close = reindex(
                price_df.pivot(index="bob", columns="symbol", values="close"),
                index=self.trade_dates,
                columns=self.stock_ids,
                fill_value=np.NaN,
            )
            H5DataSet.write_pivotDF_to_h5data(
                h5FilePath=sub_path.joinpath("stock_price.h5"),
                pivotDF=close,
                pivotKey="close",
                rewrite=True,
            )
            for coli in [
                "high",
                "low",
                "open",
                "pre_close",
                "volume",
                "amount",
            ]:
                data = reindex(
                    price_df.pivot(index="bob", columns="symbol", values=coli),
                    index=self.trade_dates,
                    columns=self.stock_ids,
                    fill_value=np.NaN,
                )
                H5DataSet.add_pivotDF_to_h5data(
                    h5FilePath=sub_path.joinpath("stock_price.h5"),
                    pivotDF=data,
                    pivotKey=coli,
                )


class back_test:
    def __init__(
        self,
        data_prepare: DataPrepare,
        signal: pd.DataFrame = None,
        method: str = "long_only",
        **kwargs
    ):
        self.group_nums = None
        self.bench_plot = False
        self.group_categorize = None
        self.result = None
        self.rtn = None
        self.weight = None
        self.back_test_data_path = data_prepare.back_test_data_path
        self.bench_code = data_prepare.bench_code
        # bench data path
        self.bench_data_path = self.back_test_data_path.joinpath(
            "bench_price", self.bench_code
        )
        # stock data path
        self.price_data_path = self.back_test_data_path.joinpath(
            "price_data", self.bench_code
        )
        self.method = method
        self.signal = reindex(
            signal, index=data_prepare.trade_dates, columns=data_prepare.stock_ids
        )

        # adjust
        self.signal_adjustment()

        # calc weight, rtn
        self.main_back_test(**kwargs)

        # result
        self.signal_result()

    def signal_adjustment(self):
        sub_path = self.back_test_data_path.joinpath("back_test")
        sub_path.mkdir(parents=True, exist_ok=True)

        self.signal = standardize(self.signal)

        H5DataSet.write_pivotDF_to_h5data(
            h5FilePath=sub_path.joinpath("adjusted_signal.h5"),
            pivotDF=self.signal,
            pivotKey="adjusted_signal",
            rewrite=True,
        )

    def signal_result(self):
        daily_rtn = self.rtn.sum(axis=1)
        self.result = curve_analysis(daily_rtn.values)
        # plot

        path = self.back_test_data_path.joinpath("back_test", "signal_result.html")
        output_file(path)

        # create a new plot
        s1 = figure(width=1200, height=600, title=None)
        s1.line(daily_rtn.index, daily_rtn.cumsum().values)
        s1.add_tools(WheelZoomTool())
        s1.xaxis.formatter = DatetimeTickFormatter(days=["%Y-%d-%d"])

        save(obj=s1, filename=path, title="outputTest")

    def main_back_test(self, **kwargs):
        sub_path = self.back_test_data_path.joinpath("back_test")
        if kwargs.get("clean_cache", False) and sub_path.exists():
            os.remove(sub_path.joinpath("weight.h5"))
        sub_path.joinpath(kwargs.get("sub_name", "")).mkdir(parents=True, exist_ok=True)

        if self.method == "long_only":
            self.weight = signal_to_weight(self.signal)
            self.bench_plot = True
            self.write_to_cache(
                self.weight, sub_path, "weight", "long_only_weight", rewrite=True
            )
        elif self.method == "top_bottom":
            self.weight = get_top_bottom_weight(
                self.signal, kwargs.get("quantile", 0.1)
            )
            self.write_to_cache(
                self.weight, sub_path, "weight", "top_bottom_weight", rewrite=True
            )
        elif self.method == "group_back_test":
            self.group_nums = kwargs.get("group_nums", 5)
            print(">> group num : {}".format(self.group_nums))
            # 分组标识
            self.group_categorize = categorize_signal_by_quantiles(
                self.signal, group_nums=self.group_nums
            )
            for group in range(0, self.group_nums):
                self.weight = signal_to_weight(
                    (self.group_categorize == group).astype(int)
                )
                self.write_to_cache(
                    self.weight,
                    sub_path,
                    "weight",
                    "group_back_test_weight_{}".format(group),
                    add=True,
                    rewrite=True,
                )
        else:
            raise AttributeError(
                "method should be longOnly, top_bottom, group_back_test"
            )
        stock_price_h5_dataSet = H5DataSet(
            self.price_data_path.joinpath("stock_price.h5")
        )
        ticker_rtn = np.divide(
            stock_price_h5_dataSet["close"] - stock_price_h5_dataSet["pre_close"],
            stock_price_h5_dataSet["pre_close"],
        )

        # ticker_rtn
        H5DataSet.write_pivotDF_to_h5data(
            h5FilePath=sub_path.joinpath("ticker_rtn.h5"),
            pivotDF=pd.DataFrame(
                data=ticker_rtn,
                index=stock_price_h5_dataSet["index"],
                columns=stock_price_h5_dataSet["columns"],
            ),
            pivotKey="ticker_rtn",
            rewrite=True,
        )

        # rtn
        self.rtn = pd.DataFrame(
            data=clean(ticker_rtn) * self.weight.values,
            index=stock_price_h5_dataSet["index"],
            columns=stock_price_h5_dataSet["columns"],
        )
        H5DataSet.write_pivotDF_to_h5data(
            h5FilePath=sub_path.joinpath("rtn.h5"),
            pivotDF=self.rtn,
            pivotKey="rtn",
            rewrite=True,
        )

    @classmethod
    def write_to_cache(
        cls,
        pivoted_data: pd.DataFrame,
        target_folder_path: Path | str,
        file_name: str,
        pivotKey_name: str,
        add=False,
        rewrite=False,
    ):
        target_folder_path = cls._format_path(target_folder_path)
        file_name = cls._format_h5_file_name(file_name)
        target_path = target_folder_path.joinpath(file_name)
        if not target_path.exists():
            # write_pivotDF_to_h5data
            H5DataSet.write_pivotDF_to_h5data(
                h5FilePath=target_path,
                pivotDF=pivoted_data,
                pivotKey=pivotKey_name,
            )
        else:
            if add:
                # add_pivotDF_to_h5data
                if pivotKey_name in H5DataSet(target_path).known_data:
                    if rewrite:
                        H5DataSet.write_pivotDF_to_h5data(
                            h5FilePath=target_path,
                            pivotDF=pivoted_data,
                            pivotKey=pivotKey_name,
                            rewrite=True,
                        )
                    else:
                        raise FileExistsError(
                            "{} is exits in {}".format(pivotKey_name, target_path)
                        )
                else:
                    H5DataSet.add_pivotDF_to_h5data(
                        h5FilePath=target_path,
                        pivotDF=pivoted_data,
                        pivotKey=pivotKey_name,
                    )
            else:
                if rewrite:
                    H5DataSet.write_pivotDF_to_h5data(
                        h5FilePath=target_path,
                        pivotDF=pivoted_data,
                        pivotKey=pivotKey_name,
                        rewrite=True,
                    )
                else:
                    raise FileExistsError("{} is exits".format(target_path))

    @staticmethod
    def _format_h5_file_name(raw_name: str):
        if raw_name.endswith(".h5"):
            return raw_name
        else:
            return raw_name + ".h5"

    @staticmethod
    def _format_path(raw_path: str | Path):
        if isinstance(raw_path, str):
            Path(raw_path).mkdir(parents=True, exist_ok=True)
            return Path(raw_path)
        else:
            raw_path.mkdir(parents=True, exist_ok=True)
            return raw_path
