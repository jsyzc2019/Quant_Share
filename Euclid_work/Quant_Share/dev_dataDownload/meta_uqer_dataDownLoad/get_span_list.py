"""
# -*- coding: utf-8 -*-
# @Time    : 2023/6/14 15:35
# @Author  : Euclid-Jie
# @File    : get_span_list.py
"""
import datetime

import pandas as pd
from Euclid_work.Quant_Share import format_date, extend_date_span


def get_span_list(
    begin, end, freq=None
) -> list[tuple[datetime.datetime, datetime.datetime, str]]:
    """
    依据freq, 对区间begin-end进行划分
    --- demo
        begin = 2018-01-04
        end = 2018-04-20
        fre = M
        ->
        [(2018-01-01, 2018-01-31, Y2018_M1), (2018-02-01, 2018-02-28, Y2018_M2), (2018-03-01, 2018-03-31, Y2018_M3), (2018-04-01, 2018-04-30, Y2018_M4)]
    ---
    """
    begin = format_date(begin)
    end = format_date(end)
    # pd.tseries.offsets 与 pd.offsets 使用无差异
    if freq in ["Y", "y"]:
        begin, end = extend_date_span(begin, end, "Y")

        span_end_list = pd.date_range(begin, end, freq="y")
        span_list = [
            (span_end - pd.offsets.YearBegin(1), span_end, "Y{}".format(span_end.year))
            for span_end in span_end_list
        ]
        return span_list

    elif freq in ["M", "m"]:
        begin, end = extend_date_span(begin, end, "M")

        span_end_list = pd.date_range(begin, end, freq="m")
        span_list = [
            (
                span_end - pd.offsets.MonthBegin(1),
                span_end,
                "Y{}_M{}".format(span_end.year, span_end.month),
            )
            for span_end in span_end_list
        ]
        return span_list

    elif freq in ["Q", "q"]:
        begin, end = extend_date_span(begin, end, "Q")
        span_end_list = pd.date_range(begin, end, freq="q")
        span_list = [
            (
                span_end - pd.offsets.QuarterBegin(n=1, startingMonth=1),
                span_end,
                "Y{}_Q{}".format(span_end.year, span_end.quarter),
            )
            for span_end in span_end_list
        ]
        return span_list
