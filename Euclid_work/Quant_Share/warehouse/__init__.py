"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/16 12:15
# @Author  : Euclid-Jie
# @File    : __init__.py.py
# @Desc    : 用于从postgresDB中读取, 写入数据
"""
from datetime import datetime, date

from pathlib import Path
from sqlalchemy import create_engine, text
from typing import Dict, Optional, List, Union
import pandas as pd
import psycopg2
from configparser import ConfigParser
import os
from ..Utils import format_date
from tqdm import tqdm

TimeType = Union[str, int, datetime, date, pd.Timestamp]


def format_table(
    table_name: str,
    columns: bool = True,
    record_time: bool = False,
    dataBase: str = None,
):
    """
    处理postgres中数据表中的列名为小写
    :param table_name: 数据表名
    :param columns: if True(default), 将对数据表的所有列名称转为小写
    :param record_time: if Ture, 会未表添加record_time列, default False
    :param dataBase: 数据库名称, 默认为ini文件中的dataBase
    """
    if table_name.lower() != table_name:
        table_name = '"{}"'.format(table_name)
    if dataBase is not None:
        conn = postgres_connect(database=dataBase)
    else:
        conn = postgres_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM {} LIMIT 0".format(table_name))
    table_column_names = [col_i.name for col_i in cur.description]
    if columns:
        for col_name_i in table_column_names:
            if col_name_i != col_name_i.lower():
                cur.execute(
                    'ALTER TABLE {} RENAME COLUMN "{}" TO {}'.format(
                        table_name, col_name_i, col_name_i.lower()
                    )
                )

    record_time_exits = "record_time" in table_column_names
    if not record_time_exits and record_time:
        cur.execute(
            "alter table {} add record_time timestamp default current_timestamp not null;".format(
                table_name
            )
        )
        print("record_time has been created in {}".format(table_name))
    conn.commit()
    cur.close()
    conn.close()


def clean_data_frame_to_postgres(
    data: pd.DataFrame, time_columns: List[str] | str = None, lower=False
):
    """
    将data frame转为可以直接写入的格式:
        - 列名一律小写
        - datetime, timestamp, date, 格式一律转为str(format = "%Y-%m-%d %H:%M:%S")
    :param data:
    :param time_columns:
    :param lower:
    """
    if time_columns is not None:
        if isinstance(time_columns, str):
            if data[time_columns].dtype != "O":
                data[time_columns] = data[time_columns].dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            for i in time_columns:
                if data[i].dtype != "O":
                    data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")
    if lower:
        data.columns = [col_i.lower() for col_i in data.columns]
    return data


def postgres_write_data_frame(
    data: pd.DataFrame,
    table_name: str,
    update: bool = False,
    unique_index: List[str] = None,
    record_time: bool = False,
    debug: bool = False,
    **kwargs,
):
    """
    更通用的postgres写入方式, pd.io.sql.to_sql适用于初次建表时写入, 后续写入时, 难免遇到重复数据, 可能导致index冲突
        重复数据: 数据内容完全一样的行, 使用append参数会导致数据库重复, 不利于维护
        index冲突: 部分表设置了unique_index, 不能接受重复的数据写入
            unique_index: 为加速取数速度, 增加的索引, 可以设置其为unique
    如何解决该冲突:
        1 忽略这部分新的重复数据, 保留原始数据
        2 用这部分重复数据覆写, 同时设置record_time标识
    注意:
        1 相比于pd.io.sql.to_sql, 此函数更通用, 建议对存在index的表示使用, 无index直接使用pd.io.sql.to_sql
        2 使用覆写时, 需要传入unique_index。同时你需要确保此表有record_time列, 当然可以帮你自动生成此列
        3 常见报错有以下几类
            1 无法识别表名或者列名: 是因为大小写问题, 建议全部转为小写
            2 无法写入日期: 确实为此函数的缺陷, 建议将data中的日期列转为str, 例如timespan(2020-01-01 20:00:00) -> "2020-01-01 20:00:00"
    :param data: 需要写入的数据
    :param table_name: 数据库名
    :param update: if True 则覆写; default False
    :param unique_index: table的unique索引列
    :param record_time: if True, 如果table没有record_time, 则未其增添
    :param debug: if Ture, 将打印insert ... 命令, 据此在sql console进行debug
    :return:
    """
    conn = postgres_connect(database=kwargs.get("database", None))
    cur = conn.cursor()
    # 判断是否有record_time列
    cur.execute("SELECT * FROM {} LIMIT 0".format(table_name))
    record_time_exits = "record_time" in [col_i.name for col_i in cur.description]
    if not record_time_exits and record_time:
        cur.execute(
            "alter table {} add record_time timestamp default current_timestamp not null;".format(
                table_name
            )
        )
        record_time_exits = True
        print("record_time has been created in {}".format(table_name))
        cur.close()

    if update:
        assert unique_index is not None, "if update data, index is needed"
        sql_texts = SQL_UPDATE_STATEMENT_FROM_DATAFRAME(
            data, table_name, unique_index, record_time_exits=record_time_exits
        )
    else:
        sql_texts = SQL_INSERT_STATEMENT_FROM_DATAFRAME(data, table_name)
    cur = conn.cursor()
    for sql_text in sql_texts:
        if debug:
            print(sql_text)
        cur.execute(sql_text)
    conn.commit()
    conn.close()


def SQL_INSERT_STATEMENT_FROM_DATAFRAME(data: pd.DataFrame, table_name: str):
    sql_texts = []
    for index, row in data.iterrows():
        sql_texts.append(
            "INSERT INTO "
            + table_name
            + " ("
            + str(", ".join(data.columns))
            + ") VALUES "
            + str(tuple(row.values)).replace("nan", "NULL").replace("None", "NULL")
            + " ON CONFLICT DO NOTHING"
        )
    return sql_texts


def postgres_cur_execute(database: str, sql_text: str):
    conn = postgres_connect(database=database)
    cur = conn.cursor()
    cur.execute(sql_text)
    conn.commit()
    cur.close()
    conn.close()


def SQL_UPDATE_STATEMENT_FROM_DATAFRAME(
    data: pd.DataFrame,
    table_name: str,
    unique_index: List[str],
    record_time_exits: bool = False,
):
    sql_texts = []
    for index, row in data.iterrows():
        sql_text = "INSERT INTO "
        sql_text += table_name
        sql_text += " ("
        sql_text += str(", ".join(data.columns))
        sql_text += ") VALUES "
        sql_text += (
            str(tuple(row.values)).replace("nan", "NULL").replace("None", "NULL")
        )
        sql_text += " ON CONFLICT ({}) DO UPDATE SET ".format(",".join(unique_index))

        for key, value in zip(data.columns, row):
            if key in unique_index:
                continue
            else:
                # TODO 肯定有什么方法可以避免此操作
                if isinstance(value, str):
                    value = value.replace("nan", "NULL").replace("None", "NULL")
                    sql_text += "{}='{}',".format(key, value)
                else:
                    value = str(value).replace("nan", "NULL").replace("None", "NULL")
                    sql_text += "{}={},".format(key, value)
        if record_time_exits:
            sql_text += "record_time=CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai'"
        else:
            sql_text = sql_text[:-1]
        sql_texts.append(sql_text)
    return sql_texts


def postgres_config(ini_filepath: str = None, section="postgresql") -> Dict:
    """
    由配置文件postgresDB.ini获取并返回config
    """
    if ini_filepath is None and os.path.exists("postgresDB.ini"):
        ini_filepath = "postgresDB.ini"
    if ini_filepath is None:
        ini_filepath = r"E:\Euclid\Quant_Share\Euclid_work\Quant_Share\test\postgresDB.ini"
        # print("default postgres config has been used")
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(ini_filepath)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(section, ini_filepath)
        )
    return db


def postgres_connect(database: str = None, config: Dict = None):
    """
    Connect to the PostgresSQL database server
    """
    if config is None:
        config = postgres_config()
    if database is not None:
        config["database"] = database
    conn = None
    try:
        # connect to the PostgresSQL server
        # print("Connecting to the PostgresSQL database...")
        conn = psycopg2.connect(**config)

        # create a cursor
        # cur = conn.cursor()

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def postgres_engine(database: str = None, config: Dict = None):
    """
    根据config返回engine, 一般供write_df_to_pgDB调用
    """
    if config is None:
        config = postgres_config()
    if database is not None:
        config["database"] = database
    return create_engine(
        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            config["user"],
            config["password"],
            config["host"],
            config["port"],
            config["database"],
        )
    )


def write_df_to_pgDB(df, table_name, engine=None, **kwargs):
    if engine is None:
        engine = postgres_engine(kwargs.get("database"), None)
    pd.io.sql.to_sql(
        df,
        table_name,
        engine,
        index=False,
        schema="public",
        if_exists="append",
    )


def load_gmData_history(
    symbols: str | list[str] = None, begin=None, end=None, adj: bool = True
):
    # TODO 进行self.stock_ids的转换, 传入筛选
    if adj:
        query = 'SELECT * FROM "gmData_history_adj"'
    else:
        query = 'SELECT * FROM "gmData_history"'
    params = {}

    if symbols is not None:
        if isinstance(symbols, str):
            symbols = [symbols]
        query += " WHERE symbol IN %(symbols)s"
        params["symbols"] = tuple(symbols)

    if begin is not None:
        begin = format_date(begin)
        if params:
            query += " AND"
        else:
            query += " WHERE"
        query += " bob >= %(begin)s"
        params["begin"] = begin

    if end is not None:
        end = format_date(end)
        if params:
            query += " AND"
        else:
            query += " WHERE"
        query += " bob <= %(end)s"
        params["end"] = end

    return pd.read_sql(query, postgres_engine(), params=params)


def load_data_from_sql(
    tableName: str,
    ticker_column: str,
    date_column: str,
    symbols: str | list[str] = None,
    begin=None,
    end=None,
):
    conn = postgres_connect()
    # 将 bob 列作为 timestamp 类型进行筛
    # TODO@AlkaidYuan, 表名带大写才需要额外引号, 可以进一步考虑一下
    sql_query = [f'SELECT * FROM "{tableName}"']
    params = {}
    date_condition = "WHERE"

    if symbols is not None:
        if isinstance(symbols, str):
            symbols = [symbols]
        sql_query.append(f"WHERE {ticker_column} IN %(symbols)s")
        params["symbols"] = tuple(symbols)
        date_condition = "AND"

    if begin is not None:
        begin = format_date(begin)
        if end is not None:
            end = format_date(end)
            sql_query.append(
                f"{date_condition} {date_column} BETWEEN %(begin)s AND %(end)s"
            )
            params.update({"begin": begin, "end": end})
        else:
            sql_query.append(f"{date_condition} {date_column} >= %(begin)s")
            params.update({"begin": begin})

    sql_query = " ".join(sql_query) + ";"

    return pd.read_sql_query(sql_query, conn, params=params)


def load_latest_sw21_data(query_date: TimeType = None):
    """
    查询距离query_date最近的申万(2021)行业分类
    该表每月底更新
    """
    if query_date is None:
        query_date = date.today()
    query_date = format_date(query_date)
    assert query_date >= pd.to_datetime(
        "20200131"
    ), "query_date should after 2020-01-31"
    if not pd.offsets.DateOffset().is_month_end(query_date):
        query_date = query_date - pd.offsets.MonthEnd(n=1)
    query = (
        "select symbol, industryid1 as industryID1 ,industryid2  as industryID2, industryid3  as industryID3, date_in, date_out , query_date from symbol_industry_sw21 as "
        "indus join industry_category_info indus_info on indus.industry_code = indus_info.industryid3 where DATE(query_date) = '{}'".format(
            query_date
        )
    )
    sw21_data = pd.read_sql(query, postgres_engine())
    if len(sw21_data) > 0:
        return sw21_data
    else:
        raise KeyError("无返回数据")
