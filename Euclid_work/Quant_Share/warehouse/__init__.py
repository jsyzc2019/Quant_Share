"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/16 12:15
# @Author  : Euclid-Jie
# @File    : __init__.py.py
# @Desc    : 用于从postgresDB中读取数据
"""
from sqlalchemy import create_engine
from typing import Dict
from Euclid_work.Quant_Share import format_date
import pandas as pd
import psycopg2
from configparser import ConfigParser
import os


def postgres_config(ini_filepath: str = None, section="postgresql") -> Dict:
    """
    由配置文件postgresDB.ini获取并返回config
    """
    if ini_filepath is None and os.path.exists("../test/postgresDB.ini"):
        ini_filepath = "../test/postgresDB.ini"
    assert ini_filepath is not None, "ini_filepath is needed!"
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


def postgres_connect(config: Dict = None):
    """
    Connect to the PostgresSQL database server
    """
    if config is None:
        config = postgres_config()
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


def postgres_engine(config: Dict = None):
    """
    根据config返回engine, 一般供write_df_to_pgDB调用
    """
    if config is None:
        config = postgres_config()
    return create_engine(
        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            config["user"],
            config["password"],
            config["host"],
            config["port"],
            config["database"],
        )
    )


def write_df_to_pgDB(df, **kwargs):
    pd.io.sql.to_sql(
        df,
        kwargs.get("table_name", "demo"),
        kwargs.get("engine"),
        index=False,
        schema="public",
        if_exists="append",
    )


def load_gmData_history(symbols: str | list[str] = None, begin=None, end=None):
    conn = postgres_connect()

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
