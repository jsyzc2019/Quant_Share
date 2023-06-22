"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/22 23:20
# @Author  : Euclid-Jie
# @File    : continuous_contracts.py
"""
from .base_package import *
from ...Utils import save_data_Y


def continuous_contracts(begin, end, **kwargs):
    if 'csymbol' not in kwargs.keys():
        raise AttributeError('csymbol should in kwargs!')

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")
    csymbol = kwargs['csymbol']
    outData = pd.DataFrame()
    with tqdm(csymbol) as t:
        t.set_description("begin:{} -- end:{}".format(begin, end))
        for patSymbol in t:
            try:
                tmpData = get_continuous_contracts(
                    csymbol=patSymbol,
                    start_date=begin,
                    end_date=end,
                )
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


def continuous_contracts_update(upDateBegin, endDate='20231231'):
    continuous_contracts_info = pd.read_excel(os.path.join(dev_files_dir, 'continuous_contracts_csymbol.xlsx'))
    csymbol = continuous_contracts_info.csymbol.tolist()
    data = continuous_contracts(begin=upDateBegin, end=endDate, csymbol=csymbol)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_data_Y(data, 'trade_date', 'continuous_contracts', reWrite=True, dataBase_root_path=dataBase_root_path_gmStockFactor)
