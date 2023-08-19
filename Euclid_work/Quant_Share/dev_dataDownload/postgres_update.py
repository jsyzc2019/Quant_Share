"""
# -*- coding: utf-8 -*-
# @Time    : 2023/8/3 21:08
# @Author  : Euclid-Jie
# @File    : postgres_update.py
# @Desc    : 用于统一数据更新的BaseClass, 目前主要用于更新Uqer数据
"""
import numpy as np
from typing import List
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime, date
from gm.api import GmError
from tqdm import tqdm

from Euclid_work.Quant_Share import format_date, get_tradeDate
from Euclid_work.Quant_Share.dev_dataDownload.meta_uqer_dataDownLoad import (
    FdmtIndiRtnPit,
    MktLimit,
    MktIdx,
    mIdxCloseWeight,
    MktEqudAdj,
    ResConSecTarpriScore,
)

from Euclid_work.Quant_Share.warehouse import (
    postgres_connect,
    postgres_engine,
    postgres_cur_execute,
    postgres_write_data_frame,
    clean_data_frame_to_postgres,
)


class postgres_update_base:
    def __init__(
        self,
        table_name: str,
        ticker_column_name: str,
        update_end_date=date.today().strftime("%Y-%m-%d"),
        ticker_list: List | str = None,
        database: str = "QS",
    ):
        """
        :param table_name: 表名
        :param database: 数据库名, 默认Quant_Share的数据都在"QS"中
        """
        self.begin_update_date_column = None
        self.single_ticker = None
        self.single_begin_date = None
        self.begin_update_date = None
        # 对于table name带有大写字母的需要加双层引号
        # prepare logger
        self.logger = self.logger_update_to_PG(table_name)
        if table_name.lower() != table_name:
            table_name = '"{}"'.format(table_name)
        self.table_name = table_name
        self.ticker_column_name = ticker_column_name
        self.update_end_date = update_end_date
        self.ticker_list = ticker_list
        self.database = database
        self.unique_index: List | str = None  # unique index name
        self.engine = postgres_engine(database=self.database)
        self.conn = postgres_connect(database=self.database)

        # clean table
        self._clean_table()

    def _clean_table(self):
        """
        对table的基本信息进行检查
        1 column是否全为小写, 并存储列名
        2 检查是否有record_time, update_time
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM {} LIMIT 0".format(self.table_name))
        self.table_column_names = [col_i.name for col_i in cur.description]
        cur.close()
        lower_table_column_names = [col_i.lower() for col_i in self.table_column_names]
        lower_or_not = np.array(lower_table_column_names) == np.array(
            self.table_column_names
        )
        assert np.all(lower_or_not), (
            ",".join(np.array(self.table_column_names)[~lower_or_not])
            + "should be lower;"
            + "\nyou can use warehouse.format_table to lower"
        )
        self.record_time_exits = "record_time" in self.table_column_names
        self.update_time_exits = "update_time" in self.table_column_names

    def load_begin(
        self, begin_update_date_column: str = None, ticker_column_name: str = None
    ):
        """
        确定上次开始更新的时间
        """
        # 全局begin date设置为同一天
        if ticker_column_name is None:
            if begin_update_date_column is not None:
                self.begin_update_date = pd.read_sql(
                    """select max(Date({})) as date from {};""".format(
                        begin_update_date_column, self.table_name
                    ),
                    con=postgres_engine(),
                ).values[0][0]
            else:
                self.begin_update_date = "2015-01-01"
                self.logger.info("begin data set to 2015-01-01")
        # 每个symbol 对应一个update begin date
        else:
            if begin_update_date_column is not None:
                self.begin_update_date = pd.read_sql(
                    """select {}, max(Date({})) as date from {} group by {};""".format(
                        ticker_column_name,
                        begin_update_date_column,
                        self.table_name,
                        ticker_column_name,
                    ),
                    con=postgres_engine(),
                )
            else:
                self.begin_update_date = "2015-01-01"
                self.logger.info("begin data set to 2015-01-01")

    @classmethod
    def logger_update_to_PG(cls, log_file_name: str, sub_path: str = "update_log"):
        sub_path = Path(sub_path)
        sub_path.mkdir(parents=True, exist_ok=True)  # if existed, pass

        logging.basicConfig(
            filename="{}/{}_{}.txt".format(
                sub_path,
                datetime.now().strftime("%Y%m%d_%H%M%S"),
                log_file_name,
            ),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger()
        return logger

    def update_time(self, time_column_name: str, ticker: str = None):
        """
        用于向QS_log中的表更新record_time, 无论是否有数据更新, 都应该更新QS_log中的对应表的record_time
        :param ticker: 需要更新的symbol
        :param time_column_name:
        """
        if ticker is None:
            postgres_cur_execute(
                database=self.database,
                sql_text="""
                UPDATE {}
                SET {} = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai'
            """.format(
                    self.table_name, time_column_name
                ),
            )
        else:
            postgres_cur_execute(
                database=self.database,
                sql_text="""
                UPDATE {}
                SET {} = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai'
                WHERE {}='{}'
            """.format(
                    self.table_name, time_column_name, self.ticker_column_name, ticker
                ),
            )

    def get_single_data(self):
        data = mIdxCloseWeight(
            begin=self.single_begin_date,
            end=self.update_end_date,
            ticker=self.single_ticker,
        )
        return clean_data_frame_to_postgres(data, lower=True)

    def main_update_to_postgres(
        self,
        begin_update_date_column: str = None,
        unique_index_columns: str | List[str] = None,
        update=True,
    ):
        self.begin_update_date_column = begin_update_date_column
        self.load_begin(begin_update_date_column, self.ticker_column_name)
        # begin update 为一天
        if isinstance(unique_index_columns, str):
            unique_index_columns = [unique_index_columns]
        if isinstance(self.begin_update_date, str):
            self.single_begin_date = self.begin_update_date
            # get single data 中不需要传ticker参数
            if self.ticker_list is None:
                # 无需loop读取
                self.t = None
                data = self.get_single_data()
                postgres_write_data_frame(
                    data,
                    table_name=self.table_name,
                    update=update,
                    unique_index=unique_index_columns,
                    record_time=self.record_time_exits,
                )
            if self.ticker_list is not None:
                with tqdm(self.ticker_list) as self.t:
                    for single_ticker in self.t:
                        self.single_ticker = single_ticker
                        self.t.set_postfix(
                            {
                                "状态": "{}:{}-{}开始获取数据...".format(
                                    single_ticker,
                                    self.begin_update_date,
                                    self.update_end_date,
                                )
                            }
                        )
                        self.single_ticker = single_ticker
                        self.save_single_data(unique_index_columns, update)
                        self.update_time(
                            time_column_name="update_time", ticker=self.single_ticker
                        )
        elif isinstance(self.begin_update_date, pd.DataFrame):
            exit_info = self.begin_update_date.set_index(self.ticker_column_name)
            with tqdm(self.ticker_list) as self.t:
                for single_ticker in self.t:
                    try:
                        begin = exit_info.loc[single_ticker]["date"].strftime(
                            "%Y-%m-%d"
                        )
                    except AttributeError:
                        # 说明目前有的表中, 有该symbol, 但是无数据, 设置其begin为2015-01-01
                        begin = "2015-01-01"
                    except KeyError:
                        # 说明目前有的表中, 没有该symbol, 设置其begin为2015-01-01
                        begin = "2015-01-01"
                        postgres_cur_execute(
                            database="QS",
                            sql_text="""
                            INSERT INTO {} ({}, {})
                            VALUES ('{}', '2015-01-01')""".format(
                                self.table_name,
                                self.ticker_column_name,
                                self.begin_update_date_column,
                                single_ticker,
                            ),
                        )

                    if format_date(begin) > get_tradeDate(self.update_end_date, -5):
                        # 设置更新周期, 相较于record time
                        self.logger.info(
                            "{}:{}-{} pass".format(
                                single_ticker, begin, self.update_end_date
                            )
                        )
                        continue
                    self.t.set_postfix(
                        {
                            "状态": "{}:{}-{}开始获取数据...".format(
                                single_ticker, begin, self.update_end_date
                            )
                        }
                    )
                    self.single_begin_date = begin
                    self.single_ticker = single_ticker
                    self.save_single_data(unique_index_columns, update)
                    self.update_time(
                        time_column_name="update_time", ticker=self.single_ticker
                    )

    def save_single_data(self, unique_index_columns, update=True):
        try:
            data = self.get_single_data()
            _len = len(data)
            if _len > 0:
                self.single_ticker = data[self.ticker_column_name].values[0]
                # 记录数据
                postgres_write_data_frame(
                    data,
                    table_name=self.table_name,
                    update=update,
                    unique_index=unique_index_columns,
                    record_time=self.record_time_exits,
                )
        except GmError:
            if self.t:
                self.t.set_postfix({"状态": "GmError:{}".format(GmError)})
            self.logger.error(
                "{}:{}-{} GmError:{}".format(
                    self.single_ticker,
                    self.single_begin_date,
                    self.update_end_date,
                    GmError,
                )
            )
            _len = -1
        except KeyError:
            _len = -1
        finally:
            if self.t:
                self.t.set_postfix(
                    {
                        "状态": "{}:{}-{}写入{}条数据".format(
                            self.single_ticker,
                            self.single_begin_date,
                            self.update_end_date,
                            _len,
                        )
                    }
                )
            self.logger.info(
                "{}:{}-{} get {} itme(s)".format(
                    self.single_ticker,
                    self.single_begin_date,
                    self.update_end_date,
                    _len,
                )
            )

    @classmethod
    def get_stock_num_list(cls):
        stockNumList = pd.read_sql(
            "select sec_id  from stock_info where delisted_date >= '2015-01-01'",
            con=postgres_engine(),
        )["sec_id"].values.tolist()
        return stockNumList

    @classmethod
    def get_bench_secID_list(cls):
        # 000905.ZICN
        benchSecIdList = pd.read_sql(
            """select distinct indexid from "uqer_MktIdx"
            """,
            con=postgres_engine(),
        )["indexid"].values.tolist()
        return benchSecIdList


class uqer_FdmtIndiRtnPit_updater(postgres_update_base):
    def __init__(
        self,
        table_name: str = "uqer_FdmtIndiRtnPit",
        ticker_column_name: str = "ticker",
        update_end_date=date.today().strftime("%Y-%m-%d"),
        ticker_list: List | str = None,
        database: str = "QS",
    ):
        super().__init__(
            table_name, ticker_column_name, update_end_date, ticker_list, database
        )

    def get_single_data(self):
        data = FdmtIndiRtnPit(
            begin=self.single_begin_date,
            end=self.update_end_date,
            ticker=self.single_ticker,
        )
        return clean_data_frame_to_postgres(data, lower=True)

    def main_update(self):
        self.main_update_to_postgres(
            begin_update_date_column="update_time",
            unique_index_columns=["ticker", "enddate"],
            update=True,
        )


class uqer_MktLimit_updater(postgres_update_base):
    def __init__(
        self,
        table_name: str = "uqer_MktLimit",
        ticker_column_name: str = "ticker",
        update_end_date=date.today().strftime("%Y-%m-%d"),
        ticker_list: List | str = None,
        database: str = "QS",
    ):
        super().__init__(
            table_name, ticker_column_name, update_end_date, ticker_list, database
        )

    def get_single_data(self):
        data = MktLimit(
            begin=self.single_begin_date,
            end=self.update_end_date,
            ticker=self.single_ticker,
        )
        data["secShortNameEn"] = data["secShortNameEn"].str.replace("'", "_")
        return clean_data_frame_to_postgres(data, lower=True)

    def main_update(self):
        self.main_update_to_postgres(
            begin_update_date_column="update_time",
            unique_index_columns=["ticker", "tradedate"],
            update=True,
        )


class uqer_MktIdx_updater(postgres_update_base):
    def __init__(
        self,
        table_name: str = "uqer_MktIdx",
        ticker_column_name: str = "ticker",
        update_end_date=date.today().strftime("%Y-%m-%d"),
        ticker_list: List | str = None,
        database: str = "QS",
    ):
        super().__init__(
            table_name, ticker_column_name, update_end_date, ticker_list, database
        )

    def get_single_data(self):
        data = MktIdx(
            begin=self.single_begin_date,
            end=self.update_end_date,
            indexID=self.single_ticker,
        )
        return clean_data_frame_to_postgres(data, lower=True)

    def main_update(self):
        self.main_update_to_postgres(
            begin_update_date_column="update_time",
            unique_index_columns=["ticker", "tradedate"],
            update=True,
        )


class uqer_mkt_equd_adj_updater(postgres_update_base):
    def __init__(
        self,
        table_name: str = "uqer_mkt_equd_adj",
        ticker_column_name: str = "ticker",
        update_end_date=date.today().strftime("%Y-%m-%d"),
        ticker_list: List | str = None,
        database: str = "QS",
    ):
        super().__init__(
            table_name, ticker_column_name, update_end_date, ticker_list, database
        )

    def get_single_data(self):
        data = MktEqudAdj(
            begin=self.single_begin_date,
            end=self.update_end_date,
            ticker=self.single_ticker,
        )
        return clean_data_frame_to_postgres(data, lower=True)

    def main_update(self):
        self.main_update_to_postgres(
            begin_update_date_column="update_time",
            unique_index_columns=["ticker", "tradedate"],
            update=True,
        )


class uqer_ResConSecTarpriScore_updater(postgres_update_base):
    def __init__(
        self,
        table_name: str = "uqer_ResConSecTarpriScore",
        ticker_column_name: str = "seccode",
        update_end_date=date.today().strftime("%Y-%m-%d"),
        ticker_list: List | str = None,
        database: str = "QS",
    ):
        super().__init__(
            table_name, ticker_column_name, update_end_date, ticker_list, database
        )

    def get_single_data(self):
        data = ResConSecTarpriScore(
            begin=self.single_begin_date,
            end=self.update_end_date,
            secCode=self.single_ticker,
        )
        return clean_data_frame_to_postgres(data, lower=True)

    def main_update(self):
        self.main_update_to_postgres(
            begin_update_date_column="updatetime",
            unique_index_columns=["seccode", "repforedate"],
            update=True,
        )


if __name__ == "__main__":
    stockNumList = postgres_update_base.get_stock_num_list()
    benchSecIdList = postgres_update_base.get_bench_secID_list()

    uqer_FdmtIndiRtnPit_updater(ticker_list=stockNumList).main_update()
    uqer_MktLimit_updater(ticker_list=stockNumList).main_update()
    uqer_MktIdx_updater(ticker_list=benchSecIdList).main_update()
    uqer_mkt_equd_adj_updater(ticker_list=stockNumList).main_update()
    uqer_ResConSecTarpriScore_updater(ticker_list=stockNumList).main_update()
