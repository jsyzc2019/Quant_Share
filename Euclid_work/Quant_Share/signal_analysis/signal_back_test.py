"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/26 19:14
# @Author  : Euclid-Jie
# @File    : signal_back_test.py
# @Desc    : 回测相关内容
"""
import os
from datetime import date, datetime
import random
from typing import Union
from bokeh.models import ColumnDataSource, DataTable, TableColumn, NumberFormatter
from bokeh.io import save  # 引入保存函数
from bokeh.layouts import gridplot
from bokeh.models import WheelZoomTool, DatetimeTickFormatter
from bokeh.palettes import Category10_10
from bokeh.plotting import figure, output_file

from .signal_utils import *
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
        self.back_test_cache_path = Path(
            data_path,
            self.begin_date.strftime("%Y%m%d") + "_" + self.end_date.strftime("%Y%m%d"),
            sub_name,
        )
        Path.mkdir(self.back_test_cache_path, exist_ok=True)
        print("cache path : {}".format(self.back_test_cache_path.resolve()))

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
                    self.back_test_cache_path.joinpath(
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
        sub_path = self.back_test_cache_path.joinpath("bench_price", self.bench_code)
        if sub_path.exists():
            print("bench_price has cache, pass")
            # load cache in back test
        else:
            print("load bench_price ...")
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
                .drop_duplicates(subset=["constickersymbol", "effdate"])
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
        sub_path = self.back_test_cache_path.joinpath("price_data", self.bench_code)
        if sub_path.exists():
            print("price_data has cache, pass")
            # H5DataSet(sub_path / "stock_price.h5").h5dir()
            # load cache
        else:
            print("load price_data ...")
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
        self.back_test_path = None  # 存放back test结果数据
        self.ticker_rtn = None  # 个股的日度收益, np.ndarray, shape(T,N), T为交易日期数, N为ticker数
        self.group_nums = None
        self.bench_plot = False
        self.group_categorize = None
        self.result = None
        self.rtn = None
        self.weight = None
        self.method = method
        self.back_test_cache_path = data_prepare.back_test_cache_path
        self.bench_code = data_prepare.bench_code
        # bench data path
        self.bench_data_path = self.back_test_cache_path.joinpath(
            "bench_price", self.bench_code
        )
        # stock data path
        self.price_data_path = self.back_test_cache_path.joinpath(
            "price_data", self.bench_code
        )
        # load stock price
        self.stock_price_h5_dataSet = H5DataSet(
            self.price_data_path.joinpath("stock_price.h5")
        )
        # get ticker rtn
        self.get_ticker_rtn()  # -> self.ticker_rtn

        # reindex signal
        self.signal = reindex(
            signal, index=data_prepare.trade_dates, columns=data_prepare.stock_ids
        )
        print(
            "back test span : {} -- {}".format(
                data_prepare.begin_date, data_prepare.end_date
            )
        )

        # adjust
        self.signal_adjustment(kwargs.get("adjustments", []))

        # calc weight, rtn
        self.main_back_test(**kwargs)

        # result
        self.signal_result()

    def signal_adjustment(self, adjustments: list[str]):
        sub_path = self.back_test_cache_path.joinpath("back_test")
        sub_path.mkdir(parents=True, exist_ok=True)
        for adjustment in adjustments:
            if adjustment == "standardize":
                self.signal = standardize(self.signal)

        H5DataSet.write_pivotDF_to_h5data(
            h5FilePath=sub_path.joinpath("adjusted_signal.h5"),
            pivotDF=self.signal,
            pivotKey="adjusted_signal",
            rewrite=True,
        )

    def signal_result(self):
        """
        绘制图像, 输出参数
        """
        rtn_H5DataSet = H5DataSet(self.back_test_path.joinpath("rtn"))
        fig_path = self.back_test_path.joinpath("signal_result.html")
        if self.method == "group_back_test":
            self.result = {}
            # 创建一个绘图对象
            rtn_fig = figure(
                width=1200,
                height=600,
                title="signal back test cum_rtn",
                x_axis_label="trade_date",
                y_axis_label="cum_rtn",
            )
            bench_rtn = H5DataSet(
                self.bench_data_path.joinpath("bench_price_pct_chg.h5")
            )["values"]
            rtn_fig.line(
                rtn_H5DataSet["index"],
                bench_rtn.cumsum(),
                legend_label="{}_cum_rtn".format(self.bench_code),
                line_color="#{:02x}{:02x}{:02x}".format(
                    random.randint(0, 255),
                    random.randint(0, 255),
                    random.randint(0, 255),
                ),
                line_width=2,
            )

            nav_fig = figure(
                width=1200,
                height=600,
                title="signal back test nav",
                x_axis_label="trade_date",
                y_axis_label="nav",
            )
            for group in range(0, self.group_nums):
                daily_rtn = rtn_H5DataSet["group_back_test_rtn_{}".format(group)].sum(
                    axis=1
                )
                self.result["group_{}".format(group)] = curve_analysis(daily_rtn)
                # plot
                # 绘制第一个线图并设置标签
                rtn_fig.line(
                    rtn_H5DataSet["index"],
                    daily_rtn.cumsum(),
                    legend_label="group_{}_cum_rtn".format(group),
                    line_color=Category10_10[group],
                    line_width=2,
                )
                # nav
                nav_fig.line(
                    rtn_H5DataSet["index"],
                    daily_rtn.cumsum() - bench_rtn.cumsum(),
                    legend_label="group_{}_nav".format(group),
                    line_color=Category10_10[group],
                    line_width=2,
                )
            # 设置图例位置
            rtn_fig.legend.location = "top_left"
            nav_fig.legend.location = "top_left"

            rtn_fig.add_tools(WheelZoomTool())
            nav_fig.add_tools(WheelZoomTool())

            rtn_fig.xaxis.formatter = DatetimeTickFormatter(days=["%Y-%d-%d"])
            nav_fig.xaxis.formatter = DatetimeTickFormatter(days=["%Y-%d-%d"])
            table = self.plot_table_result()
            output_file(fig_path)
            # 组合三个figure对象为网格排布
            grid = gridplot(
                [
                    [rtn_fig],
                    [nav_fig],
                    [table],
                ],
                toolbar_location=None,
            )
            save(obj=grid, filename=fig_path, title="output_cum_rtn")
        else:
            # long_only, top_bottom
            self.result = {}
            # 创建一个绘图对象
            rtn_fig = figure(
                width=1200,
                height=600,
                title="signal back test cum_rtn",
                x_axis_label="trade_date",
                y_axis_label="cum_rtn",
            )
            daily_rtn = rtn_H5DataSet["rtn"].sum(axis=1)
            self.result[self.method] = curve_analysis(daily_rtn)
            # plot
            # 绘制第一个线图并设置标签
            rtn_fig.line(
                rtn_H5DataSet["index"],
                daily_rtn.cumsum(),
                legend_label="cum_rtn",
                line_color="#{:02x}{:02x}{:02x}".format(
                    random.randint(0, 255),
                    random.randint(0, 255),
                    random.randint(0, 255),
                ),
                line_width=2,
            )
            if self.bench_plot and self.bench_code != "A_all":
                bench_rtn = H5DataSet(
                    self.bench_data_path.joinpath("bench_price_pct_chg.h5")
                )["values"]

                rtn_fig.line(
                    rtn_H5DataSet["index"],
                    bench_rtn.cumsum(),
                    legend_label="{}_cum_rtn".format(self.bench_code),
                    line_color="#{:02x}{:02x}{:02x}".format(
                        random.randint(0, 255),
                        random.randint(0, 255),
                        random.randint(0, 255),
                    ),
                    line_width=2,
                )
            # 设置图例位置
            rtn_fig.legend.location = "top_left"
            rtn_fig.add_tools(WheelZoomTool())
            rtn_fig.xaxis.formatter = DatetimeTickFormatter(days=["%Y-%d-%d"])
            fig_path = self.back_test_path.joinpath("signal_result.html")
            output_file(fig_path)
            table = self.plot_table_result()
            # 组合两个figure对象为网格排布
            grid = gridplot(
                [
                    [rtn_fig],
                    [table],
                ],
                toolbar_location=None,
            )
            save(obj=grid, filename=fig_path, title="output_cum_rtn")

    def plot_table_result(self):
        if self.method == "group_back_test":
            group = ["group_{}".format(group) for group in range(0, self.group_nums)]
            alzd_rtn = []
            max_down = []
            sharpe = []
            total_rtn = []
            total_std = []
            vol = []
            # 创建数据源

            for group_num in range(0, self.group_nums):
                alzd_rtn.append(self.result["group_{}".format(group_num)]["alzd_rtn"])
                max_down.append(self.result["group_{}".format(group_num)]["max_down"])
                sharpe.append(self.result["group_{}".format(group_num)]["sharpe"])
                total_rtn.append(self.result["group_{}".format(group_num)]["total_rtn"])
                total_std.append(self.result["group_{}".format(group_num)]["total_std"])
                vol.append(self.result["group_{}".format(group_num)]["vol"])
            data = dict(
                group=group,
                alzd_rtn=alzd_rtn,
                max_down=max_down,
                sharpe=sharpe,
                total_rtn=total_rtn,
                total_std=total_std,
                vol=vol,
            )
            source = ColumnDataSource(data)
            number_format = NumberFormatter(format="0,0.0000")
            # 创建TableColumn对象
            columns = [
                TableColumn(field="group", title="group"),
                TableColumn(
                    field="alzd_rtn", title="alzd_rtn", formatter=number_format
                ),
                TableColumn(
                    field="max_down", title="max_down", formatter=number_format
                ),
                TableColumn(field="sharpe", title="sharpe", formatter=number_format),
                TableColumn(
                    field="total_rtn", title="total_rtn", formatter=number_format
                ),
                TableColumn(
                    field="total_std", title="total_std", formatter=number_format
                ),
                TableColumn(field="vol", title="vol", formatter=number_format),
            ]
            # 创建DataTable对象
            data_table = DataTable(
                source=source,
                columns=columns,
                width=1200,
                height=600,
            )
            return data_table

        else:
            data = dict(
                alzd_rtn=[self.result[self.method]["alzd_rtn"]],
                max_down=[self.result[self.method]["max_down"]],
                sharpe=[self.result[self.method]["sharpe"]],
                total_rtn=[self.result[self.method]["total_rtn"]],
                total_std=[self.result[self.method]["total_std"]],
                vol=[self.result[self.method]["vol"]],
            )
            source = ColumnDataSource(data)
            number_format = NumberFormatter(format="0,0.0000")
            # 创建TableColumn对象
            columns = [
                TableColumn(
                    field="alzd_rtn", title="alzd_rtn", formatter=number_format
                ),
                TableColumn(
                    field="max_down", title="max_down", formatter=number_format
                ),
                TableColumn(field="sharpe", title="sharpe", formatter=number_format),
                TableColumn(
                    field="total_rtn", title="total_rtn", formatter=number_format
                ),
                TableColumn(
                    field="total_std", title="total_std", formatter=number_format
                ),
                TableColumn(field="vol", title="vol", formatter=number_format),
            ]
            # 创建DataTable对象
            data_table = DataTable(
                source=source, columns=columns, width=400, height=280
            )
            return data_table

    def main_back_test(self, **kwargs):
        """
        kwargs:
                clean_cache: clean weight and rtn
                sub_name: cache will write to ./back_test/sub_name/..
        """
        self.back_test_path = self.back_test_cache_path.joinpath(
            "back_test", kwargs.get("sub_name", "")
        )
        self.back_test_path.mkdir(parents=True, exist_ok=True)
        if kwargs.get("clean_cache", False) and self.back_test_path.exists():
            for cache in ["weight.h5", "rtn.h5"]:
                try:
                    os.remove(self.back_test_path.joinpath(cache))
                except FileNotFoundError:
                    pass
                    # print(
                    #     "can not find {}, pass".format(
                    #         self.back_test_path.joinpath(cache)
                    #     )
                    # )

        if self.method == "long_only":
            assert np.all(clean(self.signal.values) >= 0), "when use long_only, please assure signal >= 0"
            self.bench_plot = True  # 绘制bench rtn
            self.weight = signal_to_weight(self.signal)
            # write weight to cache
            self.write_to_cache(
                self.weight,
                self.back_test_path,
                "weight",
                "long_only_weight",
                rewrite=True,
            )
            # rtn
            self.rtn = pd.DataFrame(
                data=clean(self.ticker_rtn) * self.weight.values,
                index=self.stock_price_h5_dataSet["index"],
                columns=self.stock_price_h5_dataSet["columns"],
            )
            # write rtn to cache
            self.write_to_cache(
                self.rtn,
                self.back_test_path,
                "rtn",
                "rtn",
                add=True,
                rewrite=True,
            )
        elif self.method == "top_bottom":
            self.weight = get_top_bottom_weight(
                self.signal, kwargs.get("quantile", 0.1)
            )
            # write weight to cache
            self.write_to_cache(
                self.weight,
                self.back_test_path,
                "weight",
                "top_bottom_weight",
                rewrite=True,
            )

            # rtn
            self.rtn = pd.DataFrame(
                data=clean(self.ticker_rtn) * self.weight.values,
                index=self.stock_price_h5_dataSet["index"],
                columns=self.stock_price_h5_dataSet["columns"],
            )
            # write rtn to cache
            self.write_to_cache(
                self.rtn,
                self.back_test_path,
                "rtn",
                "rtn",
                add=True,
                rewrite=True,
            )
        elif self.method == "group_back_test":
            self.bench_plot = True  # 绘制bench rtn
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
                # write weight to cache
                self.write_to_cache(
                    self.weight,
                    self.back_test_path,
                    "weight",
                    "group_back_test_weight_{}".format(group),
                    add=True,
                    rewrite=True,
                )
                # calc rtn
                self.rtn = pd.DataFrame(
                    data=clean(self.ticker_rtn) * self.weight.values,
                    index=self.stock_price_h5_dataSet["index"],
                    columns=self.stock_price_h5_dataSet["columns"],
                )
                # write rtn to cache
                self.write_to_cache(
                    self.rtn,
                    self.back_test_path,
                    "rtn",
                    "group_back_test_rtn_{}".format(group),
                    add=True,
                    rewrite=True,
                )
        else:
            raise AttributeError(
                "method should be longOnly, top_bottom, group_back_test"
            )

    def get_ticker_rtn(self):
        """
        计算个股的日度收益, 存储至./bench_code/price_data/
        将array格式的ticker_rtn, 加入self中
        """
        self.ticker_rtn = np.divide(
            self.stock_price_h5_dataSet["close"]
            - self.stock_price_h5_dataSet["pre_close"],
            self.stock_price_h5_dataSet["pre_close"],
        )

        # ticker_rtn
        H5DataSet.write_pivotDF_to_h5data(
            h5FilePath=self.price_data_path.joinpath("ticker_rtn.h5"),
            pivotDF=pd.DataFrame(
                data=self.ticker_rtn,
                index=self.stock_price_h5_dataSet["index"],
                columns=self.stock_price_h5_dataSet["columns"],
            ),
            pivotKey="ticker_rtn",
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
