"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/26 19:13
# @Author  : Euclid-Jie
# @File    : signal_utils.py
# @Desc    : signal相关的工具函数, 同时作为Utils的补充
"""
import math
from typing import Optional, Any

import pandas as pd
import numpy as np
from pathlib import Path


def print_file_tree(directory, indent=""):
    path = Path(directory)
    if path.is_dir():
        print(f"{indent}+-- {path.name}/")
        indent += "    "
        for child in path.iterdir():
            print_file_tree(child, indent)
    else:
        print(f"{indent}+-- {path.name}")


def clean(arr: np.ndarray, inplace=False, fill_value=0.0) -> np.ndarray:
    """
    将array中的Inf, NaN转为 fill_value
    - fill_value默认值为0
    - inplace会同时改变传入的arr
    """
    assert arr.dtype == int or arr.dtype == float
    if inplace:
        res = arr
    else:
        res = arr.copy()
    res[~np.isfinite(res)] = fill_value
    return res


def signal_to_weight(signal: pd.DataFrame) -> pd.DataFrame:
    """
    将信号转为weight
    - 输入为NaN, Inf, 0的部分, 输出的weight均为0
    - 直接按照因子值大小进行赋权
        - 线性赋权 [5, 3, 2] => [0.5, 0.3, 0.2]
        -
    """
    weight_arr = signal.values.copy()
    weight_arr = clean(weight_arr)
    weight_arr = np.divide(weight_arr, weight_arr.sum(axis=1)[:, None])
    return pd.DataFrame(
        data=clean(weight_arr), index=signal.index, columns=signal.columns
    )


def categorize_signal_by_quantiles(
        signal: pd.DataFrame, group_nums: int = 5
) -> pd.DataFrame:
    """
    对信号逐行(即对每天的个股因子)进行因子分组, 默认signal越大值越大
    - 输入为NaN,Inf 输出为NaN
    - 一行全为NaN或者相同, 返回一行NaN
    - 由于使用的是sort逻辑, 会有误差
    """
    signal_arr = signal.values.copy()
    flag_all_nan_rows = ((np.abs(signal_arr) < 1e-7) | np.isnan(signal_arr)).all(axis=1)
    signal_arr[flag_all_nan_rows] = np.nan
    ranks = (signal_arr.argsort().argsort() + 1).astype(float)
    # avoid Inf, NaN
    ranks[~np.isfinite(signal_arr)] = np.nan
    # count the number of non-nan signals at any given time
    count = (~np.isnan(ranks)).sum(1)[:, None]

    result_mtx = np.full_like(signal_arr, np.nan)
    # bin_thresholds is a num_bins-1 dimensional vector, denoting the numbers separating the quantiles
    bin_thresholds = count / group_nums * np.arange(0, group_nums + 1)

    for i in range(0, group_nums):
        idx = (ranks > bin_thresholds[:, i].reshape(-1, 1)) & (
                ranks <= bin_thresholds[:, i + 1].reshape(-1, 1)
        )
        result_mtx[idx] = i
    return pd.DataFrame(data=result_mtx, index=signal.index, columns=signal.columns)


def getGroupTargPost(Score, group, ascending=True):
    """
    直接返回5组中排名中某组的pos, 已进行归一化;
    如果数据中NaN过多, 会报错
    :param ascending:
    :param Score: 评分
    :param group: 评分分组
    :return: 目标仓位
    """
    labels = ["1", "2", "3", "4", "5"]
    if not ascending:
        labels = labels[::-1]
    TargPost = (
            pd.qcut(Score, 5, labels=labels, duplicates="drop") == str(group)
    ).astype(int)
    TargPost = TargPost / TargPost.sum()
    return TargPost


def _ffill_1d(
        arr: np.ndarray, null_value: Optional[Any] = None, limit: Optional[int] = None
) -> np.ndarray:
    """Fill forward the arr matrix in the horizontal direction

    Args:

        arr: An 1D array to fill forward

        null_value: Cell with this value will be replaced with forward-filling value if possible.
            If None, the value will be set to nan

        limit: The maximum range to fill forward. nan values outside the fill forward range will keep nan

    Returns:

        The 1D array after the fill forward
    """
    assert limit is None or limit >= 0
    assert arr.ndim == 1

    original_idx = np.arange(arr.shape[0])
    # Apply the infinite fill forward
    mask = np.isnan(arr) if null_value is None else arr == null_value
    # Get the index of the last non-null_value value
    idx = np.where(~mask, original_idx, 0)

    np.maximum.accumulate(idx, out=idx)

    # Get the output matrix by querying the last non-null_value value
    out = arr[idx]

    # Set the values to null_value where the non-null_value value is too far away
    if limit is not None:
        out[original_idx - idx > limit] = np.nan if null_value is None else null_value
    return out


def _ffill_2d(
        arr: np.ndarray, null_value: Optional[Any] = None, limit: Optional[int] = None
) -> np.ndarray:
    """Fill forward the arr matrix in the vertical direction

    Args:

        arr: An 2D array to fill forward

        null_value: Cell with this value will be replaced with forward-filling value if possible.
            If None, the value will be set to nan

        limit: The maximum range to fill forward. nan values outside the fill forward range will keep nan

    Returns:

        The 2D array after the fill forward
    """
    assert limit is None or limit >= 0
    assert arr.ndim == 2

    T, N = arr.shape
    original_idx = np.arange(T)
    # Apply the infinite fill forward
    mask = np.isnan(arr) if null_value is None else arr == null_value
    # Get the index of the last non-null_value value
    idx = np.where(~mask, original_idx[:, None], 0)

    np.maximum.accumulate(idx, axis=0, out=idx)

    # Get the output matrix by querying the last non-null_value value
    out = arr[idx, np.arange(N)]

    # Set the values to null_value where the non-null_value value is too far away
    if limit is not None:
        out[original_idx[:, None] - idx > limit] = (
            np.nan if null_value is None else null_value
        )
    return out


def ffill(
        arr: np.ndarray, null_value: Optional[Any] = None, limit: Optional[int] = None
) -> np.ndarray:
    """Fill forward the arr matrix in the vertical direction

    Args:
        arr: An array to fill forward
        null_value: Cell with this value will be replaced with forward-filling value if possible.
            If None, the value will be set to nan
        limit: The maximum range to fill forward. nan values outside the fill forward range will keep nan

    Returns:
        The array after the fill forward
    """

    if arr.ndim == 1:
        return _ffill_1d(arr, null_value, limit)
    elif arr.ndim == 2:
        return _ffill_2d(arr, null_value, limit)
    else:
        raise ValueError("Only 1D and 2D array are supported Now!")


def ffillna(arr: np.ndarray, limit: Optional[int] = None) -> np.ndarray:
    """Fill forward the arr matrix in the vertical direction

    Args:

        arr: An 2D array to fill forward

        limit: The maximum range to fill forward. nan values outside the fill forward range will keep nan

    Returns:

        The 2D array after the fill forward
    """
    return ffill(arr, limit=limit)


def bfillna(arr: np.ndarray, limit=None) -> np.ndarray:
    """Fill backward the arr matrix in the vertical direction

    Args:

        arr: An 2D array to fill backward

        limit: The maximum range to fill backward. nan values outside the fill backward range will keep nan

    Returns:

        The 2D array after the fill backward
    """
    return ffillna(arr[::-1], limit=limit)[::-1]


def bfill(
        arr: np.ndarray, null_value: Optional[Any] = None, limit: Optional[int] = None
) -> np.ndarray:
    """Fill backward the arr matrix in the vertical direction

    Args:

        arr: An 2D array to fill backward

        null_value: Cell with this value will be replaced with backward-filling value if possible.
            If None, the value will be set to nan

        limit: The maximum range to fill backward. nan values outside the fill backward range will keep nan

    Returns:

        The 2D array after the fill backward
    """
    return ffill(arr[::-1], null_value=null_value, limit=limit)[::-1]


def shift(arr: np.ndarray, periods=1) -> np.ndarray:
    assert arr.dtype == float

    res = np.roll(arr, shift=periods, axis=0)
    if periods > 0:
        res[:periods] = np.nan
    else:
        res[periods:] = np.nan

    return res


def get_top_bottom_weight(signal: pd.DataFrame, quantile: float = 0.1):
    """
    对信号逐行(即对每天的个股因子)进行多空对冲的权重计算
    - quantile为top和bottom的比例, 默认为10%
    - 输入为NaN和Inf, 返回权重为0
    - top和bottom内进行等权重划分
    """
    signal_arr = signal.values.copy()
    ranks = (signal_arr.argsort().argsort() + 1).astype(float)
    ranks[~np.isfinite(signal)] = np.nan
    count = (~np.isnan(ranks)).sum(1)[:, None]
    weights = 1 / np.floor(count * quantile)
    # Get the floor of the top quantile and ceiling of the bottom quantile
    floor_top_quantile = count * (1 - quantile)
    ceil_bottom_quantile = count * quantile

    # equal size for both top group and bottom group (ceil for top,floor for bottom)
    weight_arr = weights * (
            (ranks > np.ceil(floor_top_quantile)).astype(float)
            - (ranks <= np.floor(ceil_bottom_quantile)).astype(float)
    )

    weight_arr[~np.isfinite(weight_arr)] = 0.0
    return pd.DataFrame(data=weight_arr, index=signal.index, columns=signal.columns)


def curve_analysis(rtn: np.ndarray):
    assert rtn.ndim == 1
    rtn = clean(rtn)
    nav = np.cumsum(rtn) + 1
    result = {"total_rtn": nav[-1] / nav[0] - 1}
    number_of_years = len(rtn) / 250
    result["alzd_rtn"] = result["total_rtn"] / number_of_years
    result["total_std"] = np.nanstd(rtn)
    result["vol"] = result["total_std"] * np.sqrt(250)
    result["sharpe"] = result["alzd_rtn"] / result["vol"]
    result["max_down"] = maximum_draw_down(rtn)
    return result


def maximum_draw_down(rtn: np.ndarray):
    assert rtn.ndim == 1
    min_all = 0
    sum_here = 0
    for x in rtn:
        sum_here += x
        if sum_here < min_all:
            min_all = sum_here
        elif sum_here >= 0:
            sum_here = 0
    return -min_all


def standardize(signal: pd.DataFrame) -> pd.DataFrame:
    """
    对信号逐行(即对每天的个股因子)进行标准化
    - out_signal = (signal - mean) / std
    - 计算 mean和std时 skip na
    - 输入为NaN的部分, 输出仍为NaN
    """
    signal_arr = signal.values.copy()
    signal_arr = (
                         signal_arr - np.mean(signal_arr, axis=1, where=np.isfinite(signal_arr))[:, None]
                 ) / np.std(signal_arr, axis=1, ddof=1, where=np.isfinite(signal_arr))[:, None]
    return pd.DataFrame(
        data=clean(signal_arr), index=signal.index, columns=signal.columns
    )


def demean(signal: pd.DataFrame) -> pd.DataFrame:
    """
    对信号逐行(即对每天的个股因子)进行中心化
    - out_signal = signal - mean
    - 计算 mean时 skip na
    - 输入为NaN的部分, 输出仍为NaN
    """
    pass


def demean_by_industry(
        signal: pd.DataFrame, industry_data: pd.DataFrame
) -> pd.DataFrame:
    """
    对信号逐行(即对每天的个股因子)进行中心化
    - industry_data元素为行业编号, 即: SW21_Level1为0-30
    - industry的index和columns与signal保持一致
    - out_signal = signal - mean_i
    - 计算 mean时 skip na
    - 输入为NaN的部分, 输出仍为NaN
    """
    pass
