"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/15 9:54
# @Author  : Euclid-Jie
# @File    : save_gm_dataQ.py
"""
from base_package import *


def save_gm_dataQ(df, date_column_name, tableName, dataBase_root_path=dataBase_root_path_gmStockFactor, reWrite=False):
    print("数据将存储在: {}/{}".format(dataBase_root_path, tableName))
    df["year"] = df[date_column_name].apply(lambda x: format_date(x).year)
    df["quarter"] = df[date_column_name].apply(lambda x: format_date(x).quarter)
    for yeari in range(df["year"].min(), df["year"].max() + 1):
        df1 = df[df["year"] == yeari]
        for quarteri in range(df1["quarter"].min(), df1["quarter"].max() + 1):
            df2 = df1[df1["quarter"] == quarteri]
            df2 = df2.drop(columns=['year', 'quarter'], axis=1)
            save_data_h5(df2, name='{}_Y{}_Q{}'.format(tableName, yeari, quarteri),
                         subPath="{}/{}".format(dataBase_root_path, tableName), reWrite=reWrite)
