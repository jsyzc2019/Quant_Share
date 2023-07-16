"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/16 12:15
# @Author  : Euclid-Jie
# @File    : __init__.py.py
# @Desc    : 用于从postgresDB中读取数据
"""
from sqlalchemy import create_engine
from Euclid_work.Quant_Share import format_date
import pandas as pd
import psycopg2


def pgConnect(**kwargs):
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database=kwargs.get("database", "postgres"),
        user=kwargs.get("user", "readonly"),
        password=kwargs.get("password", "read123456"),
    )


def load_gmData_history(symbols: str | list[str] = None, begin=None, end=None):
    conn = pgConnect(database="QS")
    # 将 bob 列作为 timestamp 类型进行筛
    if symbols is not None:
        if isinstance(symbols, str):
            symbols = [symbols]
        if begin is not None:
            begin = format_date(begin)
            if end is not None:
                end = format_date(end)
                query = """
                    SELECT *
                    FROM "gmData_history"
                    WHERE symbol IN %(symbols)s
                    AND bob BETWEEN %(begin)s AND %(end)s;
                """
                params = {"symbols": tuple(symbols), "begin": begin, "end": end}
                return pd.read_sql_query(query, conn, params=params)
            else:
                query = """
                    SELECT *
                    FROM "gmData_history"
                    WHERE symbol IN %(symbols)s
                    AND bob >= %(begin)s;
                """
                params = {"symbols": tuple(symbols), "begin": begin}
                return pd.read_sql_query(query, conn, params=params)
        else:
            query = """
                SELECT *
                FROM "gmData_history"
                WHERE symbol IN %(symbols)s
            """
            params = {"symbols": tuple(symbols)}
            return pd.read_sql_query(query, conn, params=params)
    else:
        if begin is not None:
            begin = format_date(begin)
            if end is not None:
                end = format_date(end)
                query = """
                    SELECT *
                    FROM "gmData_history"
                    WHERE bob BETWEEN %(begin)s AND %(end)s;
                """
                params = {"begin": begin, "end": end}
                return pd.read_sql_query(query, conn, params=params)
            else:
                query = """
                    SELECT *
                    FROM "gmData_history"
                    WHERE bob >= %(begin)s;
                """
                params = {"begin": begin}
                return pd.read_sql_query(query, conn, params=params)
        else:
            query = """
                SELECT *
                FROM "gmData_history"
            """
            return pd.read_sql_query(query, conn)

def load_data_from_sql(
        tableName:str,
        ticker_column: str,
        date_column: str,
        symbols: str | list[str] = None,
        begin=None,
        end=None):
    conn = pgConnect(database="QS")
    # 将 bob 列作为 timestamp 类型进行筛

    sql_query = [f'SELECT * FROM "{tableName}"']
    params = {}
    date_condition = 'WHERE'

    if symbols is not None:
        if isinstance(symbols, str):
            symbols = [symbols]
        sql_query.append(f"WHERE {ticker_column} IN %(symbols)s")
        params["symbols"] = tuple(symbols)
        date_condition = 'AND'

    if begin is not None:
        begin = format_date(begin)
        if end is not None:
            end = format_date(end)
            sql_query.append(f"{date_condition} {date_column} BETWEEN %(begin)s AND %(end)s")
            params.update({"begin": begin, "end": end})
        else:
            sql_query.append(f"{date_condition} {date_column} >= %(begin)s")
            params.update({"begin": begin})

    sql_query = " ".join(sql_query) + ";"

    return pd.read_sql_query(sql_query, conn, params=params)