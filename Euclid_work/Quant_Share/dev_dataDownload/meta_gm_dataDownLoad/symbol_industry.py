from .base_package import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from Euclid_work.Quant_Share import tradeDateList
import pandas as pd
from time import sleep
from tqdm import tqdm
import re


def run_thread_pool_sub(target, args, max_work_count):
    with ThreadPoolExecutor(max_workers=max_work_count) as t:
        res = [t.submit(target, i) for i in args]
    return res


def get_industry_info(date):
    res = pd.DataFrame()
    update_exit = 0
    symbols = get_symbols(sec_type1=1010, df=True, trade_date=date)["symbol"].tolist()
    # t.set_description(f"正在获取{td}的数据")
    for i, s in enumerate(symbols):
        try:
            df = stk_get_symbol_industry(symbols=s, source="sw2021", level=1, date=date)
            # t.set_postfix({"状态": "symbol {}成功获取, total {:.2f}%".format(s, (i+1)/len(symbols)*100)})
            _len = len(df)
            if _len > 0:
                update_exit = 0
                res = pd.concat([res, df], axis=0, ignore_index=True)
                sleep(0.5)
            else:
                update_exit += 1
            if update_exit >= update_exit_limit:
                print("no data return, exit update")
                break

        except GmError:
            # t.set_postfix({"状态": "symbol {}无效, total {:.2f}%".format(s, (i+1)/len(symbols)*100)})
            continue
    return res


def stk_symbol_industry(begin="20150101", end=None):
    end = end if end else date.today()
    tradeDays = pd.date_range(begin, end).intersection(tradeDateList)
    tradeDays = tradeDays.strftime("%Y-%m-%d")
    res = pd.DataFrame()
    ics = run_thread_pool_sub(get_industry_info, tradeDays, max_work_count=10)
    for ic in as_completed(ics):
        df = ic.result()
        if len(res) > 0:
            res = pd.concat([res, df], axis=0, ignore_index=True)
    return res


re_symbol = re.compile("[A-Z]{4}.[0-9]+")


def get_info_loop(symbols, td):
    try:
        df = stk_get_symbol_industry(symbols=symbols, source="sw2021", level=1, date=td)
        return df
    except GmError as e:
        wrong_symbol = re_symbol.search(str(e)).group()
        symbols.remove(wrong_symbol)
        print(f"symbol {wrong_symbol} has been removed")
        get_info_loop(symbols, td)


def symbol_industry(begin="20150101", end=None):
    end = end if end else date.today()
    tradeDays = pd.date_range(begin, end).intersection(tradeDateList)
    tradeDays = tradeDays.strftime("%Y-%m-%d")
    industry_category = stk_get_industry_category(source="sw2021", level=1)[
        "industry_code"
    ]
    res = pd.DataFrame()
    errors_num = 0
    with tqdm(tradeDays) as t:
        for td in t:
            t.set_description(f"正在获取{td}的数据")
            for sc in industry_category:
                try:
                    df = stk_get_industry_constituents(sc, date=td)
                    df["query_date"] = td
                    res = pd.concat([res, df], axis=0, ignore_index=True)
                    errors_num = 0
                    t.set_postfix({"状态": f"{sc}成功获取"})
                except GmError:
                    t.set_postfix({"状态": f"{sc}未成功获取"})
                    errors_num += 1
                    if errors_num > 5:
                        raise RuntimeError("重试五次后，仍旧GmError")
                    time.sleep(60)
                    t.set_postfix(
                        {"状态": "GmError:{}, 将第{}次重试".format(GmError, errors_num)}
                    )
    return res


def symbol_industry_update(upDateBegin="20150101", end=None):
    data = symbol_industry(begin=upDateBegin, end=end)
    save_data_Y(
        data,
        "query_date",
        "symbol_industry",
        reWrite=True,
        _dataBase_root_path=dataBase_root_path_gmStockFactor,
    )
