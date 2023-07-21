"""
# -*- coding: utf-8 -*-
# @Time    : 2023/7/21 17:56
# @Author  : Euclid-Jie
# @File    : balance_sheet_update_to_PG.py
# @Desc    : 更新_balance_sheet, 基于record_time
"""
from Euclid_work.Quant_Share.warehouse import *
from Euclid_work.Quant_Share import get_tradeDate
from meta_gm_dataDownLoad import *
from datetime import datetime, date
import logging

logging.basicConfig(
    filename="log_balance_sheet_update_{}.txt".format(
        datetime.now().strftime("%Y%m%d_%H%M%S")
    ),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

balance_sheet_info = pd.read_excel(os.path.join(dev_files_dir, "balance_sheet.xlsx"))
balance_sheet_fields = balance_sheet_info["列名"].to_list()

# 获取数据库中已有数据
exit_info = pd.read_sql(
    "select symbol, max(Date(record_time)) as date from balance_sheet group by symbol",
    con=postgres_engine(),
)
exit_info = exit_info.set_index("symbol")
with tqdm(symbolList[73:]) as t:
    end = date.today().strftime("%Y-%m-%d")
    for symbol in t:
        try:
            begin = exit_info.loc[symbol]["date"].strftime("%Y-%m-%d")
        except KeyError:
            # 一般认为这种数据表中没有的symbol为2015-01-01前就退市, 可以直接continue, 不用获取数据
            # begin = "2015-01-01"
            continue

        if format_date(begin) > get_tradeDate(end, -5):
            logger.info("{}:{}-{} pass".format(symbol, begin, end))
            continue
        t.set_postfix({"状态": "{}:{}-{}开始获取数据...".format(symbol, begin, end)})
        try:
            data = get_fundamentals(
                table="balance_sheet",
                symbols=symbol,
                limit=1000,
                start_date=begin,
                end_date=end,
                fields=balance_sheet_fields,
                df=True,
            )
            if len(data) > 0:
                # TODO 贼离谱, 查出来的symbol并不是query中的symbol
                symbol = data["symbol"].values[0]
                for i in ["pub_date", "end_date"]:
                    data[i] = data[i].dt.strftime("%Y-%m-%d %H:%M:%S")
                data.columns = [col_i.lower() for col_i in data.columns]
                postgres_write_data_frame(
                    data,
                    "balance_sheet",
                    update=True,
                    unique_index=["symbol", "pub_date", "end_date"],
                    record_time=True,
                )
        except GmError:
            t.set_postfix({"状态": "GmError:{}".format(GmError)})
            logger.error("{}:{}-{} GmError:{}".format(symbol, begin, end, GmError))
            continue
        finally:
            t.set_postfix(
                {"状态": "{}:{}-{}写入{}条数据".format(symbol, begin, end, len(data))}
            )
            logger.info("{}:{}-{} get {} itme(s)".format(symbol, begin, end, len(data)))
