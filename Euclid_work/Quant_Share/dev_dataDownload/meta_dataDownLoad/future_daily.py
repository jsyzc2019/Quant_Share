"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/22 23:21
# @Author  : Euclid-Jie
# @File    : future_daily.py
"""

from .base_package import *


def future_daily(**kwargs):
    if 'tradeDateArr' not in kwargs.keys():
        raise AttributeError('tradeDateArr should in kwargs!')

    tradeDateArr = kwargs['tradeDateArr']
    outData = pd.DataFrame()
    with tqdm(tradeDateArr) as t:
        for patSymbol in t:
            t.set_description("trade date:{}".format(patSymbol))
            try:
                tmpData = get_symbols(1040, df=True, trade_date=patSymbol.strftime('%Y-%m-%d'))
                t.set_postfix({"状态": "已成功获取{}条数据".format(len(tmpData))})  # 进度条右边显示信息
                errors_num = 0
                if len(tmpData) == 0:
                    continue
            except GmError:
                errors_num += 1
                if errors_num > 5:
                    raise RuntimeError("重试五次后，仍旧GmError")
                time.sleep(60)
                t.set_postfix({"状态": "GmError:{}, 将第{}次重试".format(GmError, errors_num)})
            tmpData = pd.DataFrame(tmpData)
            tmpData.trade_date = tmpData.trade_date.dt.strftime('%Y-%m-%d')
            outData = pd.concat([outData, tmpData], ignore_index=True, axis=0)
    return outData


if __name__ == '__main__':
    begin = format_date('20150101')
    end = format_date('20231231')
    tradeDateArr = [i for i in tradeDateList if begin <= i <= end]
    data = future_daily(tradeDateArr=tradeDateArr)
    save_gm_data_Y(data, 'trade_date', 'future_daily', reWrite=True)
