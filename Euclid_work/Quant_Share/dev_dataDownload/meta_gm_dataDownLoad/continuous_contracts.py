"""
# -*- coding: utf-8 -*-
# @Time    : 2023/5/22 23:20
# @Author  : Euclid-Jie
# @File    : continuous_contracts.py
"""
from .base_package import *


def continuous_contracts(begin, end, **kwargs):
    if "csymbol" not in kwargs.keys():
        raise AttributeError("csymbol should in kwargs!")

    begin = format_date(begin).strftime("%Y-%m-%d")
    end = format_date(end).strftime("%Y-%m-%d")
    csymbol = kwargs["csymbol"]
    outData = pd.DataFrame()
    errors_num = 0
    update_exit = 0
    with tqdm(csymbol) as t:
        t.set_description("begin:{} -- end:{}".format(begin, end))
        for patSymbol in t:
            try:
                tmpData = get_continuous_contracts(
                    csymbol=patSymbol,
                    start_date=begin,
                    end_date=end,
                )
                _len = len(tmpData)
                t.set_postfix({"状态": "已成功获取{}条数据".format(_len)})  # 进度条右边显示信息
                errors_num = 0

                if _len > 0:
                    update_exit = 0
                    tmpData = pd.DataFrame(tmpData)
                    tmpData.trade_date = tmpData.trade_date.dt.strftime("%Y-%m-%d")
                    outData = pd.concat([outData, tmpData], ignore_index=True, axis=0)
                else:
                    update_exit += 1
                if update_exit >= update_exit_limit:
                    print("no data return, exit update")
                    break

            except GmError:
                errors_num += 1
                if errors_num > 5:
                    raise RuntimeError("重试五次后，仍旧GmError")
                time.sleep(60)
                t.set_postfix({"状态": "GmError:{}, 将第{}次重试".format(GmError, errors_num)})

    return outData


def continuous_contracts_update(upDateBegin, endDate="20231231"):
    continuous_contracts_info = pd.read_excel(
        os.path.join(dev_files_dir, "continuous_contracts_csymbol.xlsx")
    )
    csymbol = continuous_contracts_info.csymbol.tolist()
    data = continuous_contracts(begin=upDateBegin, end=endDate, csymbol=csymbol)
    if len(data) == 0:
        print("无数据更新")
    else:
        save_data_Y(
            data,
            "trade_date",
            "continuous_contracts",
            reWrite=True,
            _dataBase_root_path=dataBase_root_path_gmStockFactor,
        )
