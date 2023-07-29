from datetime import date
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
import math
from scipy.stats.mstats import winsorize


# 基础科目及衍生类因子


# 净运营资本(net_working_capital)，stock_file为所需数据dataframe
# =流动资产 (ttl_cur_ast)－ 流动负债(ttl_cur_liab)
def net_working_capital(stock_file):
    stock_file = stock_file.copy()
    stock_file["net_working_capital"] = (
        stock_file["ttl_cur_ast"] - stock_file["ttl_cur_liab"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_working_capital"]]
    return stock_file


# 营业总收入TTM(total_operating_revenue_ttm),stock_file为所需数据dataframe
# 过去12个月的 营业总收入(ttl_inc_oper) 之和
def total_operating_revenue_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["total_operating_revenue_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_operating_revenue_ttm"]
    ]
    return stock_file


# 营业利润TTM operating_profit_ttm,stock_file为所需数据dataframe
# 过去12个月 营业利润(oper_prof) 之和
def operating_profit_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["operating_profit_ttm"] = (
        stock_file.groupby(["symbol"])["oper_prof"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "operating_profit_ttm"]]
    return stock_file


# 经营活动现金流量净额TTM net_operate_cash_flow_ttm,stock_file为所需数据dataframe
# 过去12个月 经营活动产生的现金流量净值(net_cf_oper) 之和
def net_operate_cash_flow_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_operate_cash_flow_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cash_flow_ttm"]
    ]
    return stock_file


# 营业收入TTM operating_revenue_ttm,stock_file为所需数据dataframe
# 过去12个月 营业收入(inc_oper) 之和
def operating_revenue_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["operating_revenue_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "operating_revenue_ttm"]]
    return stock_file


# 带息流动负债(interest_carry_current_liability)，stock_file为所需数据dataframe
# =流动负债合计(ttl_cur_liab) - 无息流动负债(interest_free_current_liability)
def interest_carry_current_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file1 = interest_free_current_liability(stock_file)
    stock_file = pd.merge(stock_file, stock_file1, on=["symbol", "rpt_date"])
    stock_file["interest_carry_current_liability"] = (
        stock_file["ttl_cur_liab"] - stock_file["interest_free_current_liability"]
    )
    stock_file["pub_date"] = stock_file[["pub_date_x", "pub_date_y"]].max(axis=1)
    stock_file.drop(["pub_date_x", "pub_date_y"], axis=1, inplace=True)
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "interest_carry_current_liability"]
    ]
    return stock_file


# 销售费用TTM sale_expense_ttm,stock_file为所需数据dataframe
# 过去12个月 销售费用(exp_sell) 之和
def sale_expense_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["sale_expense_ttm"] = (
        stock_file.groupby(["symbol"])["exp_sell"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "sale_expense_ttm"]]
    return stock_file


# 毛利TTM gross_profit_ttm,stock_file为所需数据dataframe
# 过去12个月 毛利润(grossProfit) 之和  毛利润=营业收入 inc_oper-营业成本 cost_oper
def gross_profit_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["grossProfit"] = stock_file["inc_oper"] - stock_file["cost_oper"]
    stock_file["gross_profit_ttm"] = (
        stock_file.groupby(["symbol"])["grossProfit"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "gross_profit_ttm"]]
    return stock_file


# 留存收益(retained_earnings)，stock_file为所需数据dataframe
# =盈余公积金(sur_rsv)+未分配利润(ret_prof)
def retained_earnings(stock_file):
    stock_file = stock_file.copy()
    stock_file["retained_earnings"] = stock_file["sur_rsv"] + stock_file["ret_prof"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "retained_earnings"]]
    return stock_file


# 营业总成本TTM total_operating_cost_ttm,stock_file为所需数据dataframe
# 过去12个月 营业总成本(ttl_cost_oper) 之和
def total_operating_cost_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["total_operating_cost_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_cost_oper"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_operating_cost_ttm"]
    ]
    return stock_file


# 营业外收支净额TTM non_operating_net_profit_ttm,stock_file为所需数据dataframe
# 营业外收入（TTM） (inc_noper)- 营业外支出（TTM）(exp_noper)
def non_operating_net_profit_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_noper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_noper"].rolling(window=4).sum().values
    )
    stock_file["exp_noper_ttm"] = (
        stock_file.groupby(["symbol"])["exp_noper"].rolling(window=4).sum().values
    )
    stock_file["non_operating_net_profit_ttm"] = (
        stock_file["inc_noper_ttm"] - stock_file["exp_noper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "non_operating_net_profit_ttm"]
    ]
    return stock_file


# 投资活动现金流量净额TTM net_invest_cash_flow_ttm,stock_file为所需数据dataframe
# 过去12个月 投资活动现金流量净额(net_cf_inv) 之和
def net_invest_cash_flow_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_invest_cash_flow_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_inv"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_invest_cash_flow_ttm"]
    ]
    return stock_file


# 财务费用TTM financial_expense_ttm,stock_file为所需数据dataframe
# 过去12个月 财务费用(fin_exp)  之和
def financial_expense_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["financial_expense_ttm"] = (
        stock_file.groupby(["symbol"])["fin_exp"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "financial_expense_ttm"]]
    return stock_file


# 管理费用TTM administration_expense_ttm,stock_file为所需数据dataframe
# 过去12个月 管理费用(exp_adm)  之和
def administration_expense_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["administration_expense_ttm"] = (
        stock_file.groupby(["symbol"])["exp_adm"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "administration_expense_ttm"]
    ]
    return stock_file


# 净利息费用(net_interest_expense)，stock_file为所需数据dataframe
# =利息支出(exp_int)-利息收入(inc_int)
def net_interest_expense(stock_file):
    stock_file = stock_file.copy()
    stock_file["net_interest_expense"] = stock_file["exp_int"] - stock_file["inc_int"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_interest_expense"]]
    return stock_file


# 价值变动净收益TTM value_change_profit_ttm,stock_file为所需数据dataframe
# 过去12个月 价值变动净收益(NVALCHGIT)  之和
def value_change_profit_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["value_change_profit_ttm"] = (
        stock_file.groupby(["symbol"])["NVALCHGIT"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "value_change_profit_ttm"]
    ]
    return stock_file


# 利润总额TTM total_profit_ttm,stock_file为所需数据dataframe
# 过去12个月 利润总额(ttl_prof) 之和
def total_profit_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["total_profit_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_prof"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "total_profit_ttm"]]
    return stock_file


# 筹资活动现金流量净额TTM net_finance_cash_flow_ttm,stock_file为所需数据dataframe
# 过去12个月 筹资活动现金流量净额(net_cf_fin) 之和
def net_finance_cash_flow_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_finance_cash_flow_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_fin"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_finance_cash_flow_ttm"]
    ]
    return stock_file


# 无息流动负债(interest_free_current_liability)，stock_file为所需数据dataframe
# =应付票据(note_pay)+应付账款(acct_pay)+预收账款(用 预收款项 代替,adv_acct)+应交税费(tax_pay)+应付利息(int_pay)+其他应付款(oth_pay)+其他流动负债(oth_cur_liab)
def interest_free_current_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file["interest_free_current_liability"] = (
        stock_file["note_pay"]
        + stock_file["acct_pay"]
        + stock_file["adv_acct"]
        + stock_file["tax_pay"]
        + stock_file["int_pay"]
        + stock_file["oth_pay"]
        + stock_file["oth_cur_liab"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "interest_free_current_liability"]
    ]
    return stock_file


# 息税前利润(EBIT)，stock_file为所需数据dataframe
# =净利润(net_prof)+所得税(inc_tax)+财务费用(fin_exp)
def EBIT(stock_file):
    stock_file = stock_file.copy()
    stock_file["EBIT"] = (
        stock_file["fin_exp"] + stock_file["net_prof"] + stock_file["inc_tax"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EBIT"]]
    return stock_file


# 净利润TTM net_profit_ttm,stock_file为所需数据dataframe
# 过去12个月 净利润(net_prof) 之和
def net_profit_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_profit_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_profit_ttm"]]
    return stock_file


# 经营活动净收益 (OperateNetIncome),stock_file为所需数据dataframe
# =经营活动净收益=ttl_inc_oper营业总收入-ttl_cost_oper营业总成本
def OperateNetIncome(stock_file):
    stock_file = stock_file.copy()
    stock_file["OperateNetIncome"] = (
        stock_file["ttl_inc_oper"] - stock_file["ttl_cost_oper"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "OperateNetIncome"]]
    return stock_file


# 息税折旧摊销前利润 (EBITDA),stock_file为所需数据dataframe
# （营业总收入ttl_inc_oper-营业税金及附加biz_tax_sur）
# -（营业成本cost_oper+利息支出exp_int+手续费及佣金支出exp_fee_comm+销售费用exp_sell+管理费用exp_adm+研发费用exp_rd+资产减值损失ast_impr_loss）
# +（固定资产折旧、油气资产折耗、生产性生物资产折旧depr_oga_cba）+无形资产摊销amort_intg_ast+长期待摊费用摊销amort_lt_exp_ppay;
def EBITDA(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EBITDA"] = (
        stock_file["ttl_inc_oper"]
        - stock_file["biz_tax_sur"]
        - (
            stock_file["cost_oper"]
            + stock_file["exp_int"]
            + stock_file["exp_fee_comm"]
            + stock_file["exp_sell"]
            + stock_file["exp_adm"]
            + stock_file["exp_rd"]
            + stock_file["ast_impr_loss"]
        )
        + stock_file["depr_oga_cba"]
        + stock_file["amort_intg_ast"]
        + stock_file["amort_lt_exp_ppay"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EBITDA"]]
    return stock_file


# 资产减值损失TTM asset_impairment_loss_ttm,stock_file为所需数据dataframe
# 过去12个月 资产减值损失(ast_impr_loss) 之和
def asset_impairment_loss_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["asset_impairment_loss_ttm"] = (
        stock_file.groupby(["symbol"])["ast_impr_loss"].rolling(window=4).sum().values
    )
    stock_file["asset_impairment_loss_ttm"] = (
        -1 * stock_file["asset_impairment_loss_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "asset_impairment_loss_ttm"]
    ]
    return stock_file


# 归属于母公司股东的净利润TTM np_parent_company_owners_ttm,stock_file为所需数据dataframe
# 过去12个月 归属于母公司股东的净利润(net_prof_pcom) 之和
def np_parent_company_owners_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["np_parent_company_owners_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof_pcom"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "np_parent_company_owners_ttm"]
    ]
    return stock_file


# 营业成本TTM operating_cost_ttm,stock_file为所需数据dataframe
# 过去12个月 营业成本(cost_oper) 之和
def operating_cost_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["operating_cost_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "operating_cost_ttm"]]
    return stock_file


# 净债务(net_debt),stock_file为所需数据dataframe
# =总债务(TDEBT)-期末现金及现金等价物余额 cash_cash_eq_end
def net_debt(stock_file):
    stock_file = stock_file.copy()
    stock_file["net_debt"] = stock_file["TDEBT"] - stock_file["cash_cash_eq_end"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_debt"]]
    return stock_file


# 非经常性损益 (non_recurring_gain_loss),stock_file为所需数据dataframe
# =归属于母公司股东的净利润(net_prof_pcom)-扣除非经常损益后的净利润 NPCUT(元)
def non_recurring_gain_loss(stock_file):
    stock_file = stock_file.copy()
    stock_file["non_recurring_gain_loss"] = (
        stock_file["net_prof_pcom"] - stock_file["NPCUT"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "non_recurring_gain_loss"]
    ]
    return stock_file


# 销售商品提供劳务收到的现金 goods_sale_and_service_render_cash_ttm,stock_file为所需数据dataframe
# 过去12个月 销售商品提供劳务收到的现金(cash_rcv_sale) 之和
def goods_sale_and_service_render_cash_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["goods_sale_and_service_render_cash_ttm"] = (
        stock_file.groupby(["symbol"])["cash_rcv_sale"].rolling(window=4).sum().values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "goods_sale_and_service_render_cash_ttm"]
    ]
    return stock_file


# 市值 (market_cap),stock_file为所需数据dataframe
# =市值=share_total总股本*close收盘价
def market_cap(stock_file):
    stock_file = stock_file.copy()
    stock_file["market_cap"] = stock_file["share_total"] * stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "market_cap"]]
    return stock_file


# 现金流市值比cash_flow_to_price_ratio  1 / pcf_ratio (ttm)
# pcf_ratio (ttm)=PCTTM（当日收盘价＊当日公司总股本／经营活动产生的现金流，其中经营活动产生的现金流取最近四个季度的）
def cash_flow_to_price_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cash_flow_to_price_ratio"] = 1 / stock_file["PCTTM"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "cash_flow_to_price_ratio"]
    ]
    return stock_file


# 营收市值比sales_to_price_ratio 1 / ps_ratio (ttm)
# ps_ratio (ttm)=PSTTM（当日收盘价＊当日公司总股本／营业收入，其中营业收入取最近四个季度的）
def sales_to_price_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["sales_to_price_ratio"] = 1 / stock_file["PSTTM"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "sales_to_price_ratio"]]
    return stock_file


# 流通市值 (circulating_market_cap),stock_file为所需数据dataframe
# =流通市值=share_circ流通股本*close收盘价
def circulating_market_cap(stock_file):
    stock_file = stock_file.copy()
    stock_file["circulating_market_cap"] = (
        stock_file["share_circ"] * stock_file["close"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "circulating_market_cap"]
    ]
    return stock_file


# 经营性资产 (operating_assets),stock_file为所需数据dataframe
# =总资产(ttl_ast) - 金融资产(货币资金(mny_cptl)+交易性金融资产(trd_fin_ast)+应收票据(note_rcv)
# +应收利息(int_rcv)+应收股利(dvd_rcv)+可供出售金融资产(aval_sale_fin) +持有至到期投资(htm_inv))
def operating_assets(stock_file):
    stock_file = stock_file.copy()
    stock_file["operating_assets"] = stock_file["ttl_ast"] - (
        stock_file["mny_cptl"]
        + stock_file["trd_fin_ast"]
        + stock_file["note_rcv"]
        + stock_file["int_rcv"]
        + stock_file["dvd_rcv"]
        + stock_file["aval_sale_fin"]
        + stock_file["htm_inv"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "operating_assets"]]
    return stock_file


# 金融资产 (financial_assets),stock_file为所需数据dataframe
# =货币资金(mny_cptl)+交易性金融资产(trd_fin_ast)+应收票据(note_rcv)+应收利息(int_rcv)+应收股利(dvd_rcv)+可供出售金融资产(aval_sale_fin) +持有至到期投资(htm_inv)
def financial_assets(stock_file):
    stock_file = stock_file.copy()
    stock_file["financial_assets"] = (
        stock_file["mny_cptl"]
        + stock_file["trd_fin_ast"]
        + stock_file["note_rcv"]
        + stock_file["int_rcv"]
        + stock_file["dvd_rcv"]
        + stock_file["aval_sale_fin"]
        + stock_file["htm_inv"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "financial_assets"]]
    return stock_file


# 经营性负债 operating_liability,stock_file为所需数据dataframe
# 总负债(ttl_liab) - 金融负债
# 金融负债=(流动负债合计 ttl_cur_liab-应付账款 acct_pay-预收款项 adv_acct-应付职工薪酬 emp_comp_pay-应交税费 tax_pay
# -其他应付款 oth_pay-一年内的递延收益DEFEREVE-其它流动负债 oth_cur_liab)+(长期借款 lt_ln+应付债券 bnd_pay)
def operating_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file["operating_liability"] = stock_file["ttl_liab"] - (
        stock_file["ttl_cur_liab"]
        - stock_file["acct_pay"]
        - stock_file["adv_acct"]
        - stock_file["emp_comp_pay"]
        - stock_file["tax_pay"]
        - stock_file["oth_pay"]
        - stock_file["DEFEREVE"]
        - stock_file["oth_cur_liab"]
        + stock_file["lt_ln"]
        + stock_file["bnd_pay"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "operating_liability"]]
    return stock_file


# 金融负债 (financial_liability)
# (流动负债合计 ttl_cur_liab-应付账款 acct_pay-预收款项 adv_acct-应付职工薪酬 emp_comp_pay-应交税费 tax_pay
# -其他应付款 oth_pay-一年内的递延收益DEFEREVE-其它流动负债 oth_cur_liab)+(长期借款 lt_ln+应付债券 bnd_pay)
def financial_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file["financial_liability"] = (
        stock_file["ttl_cur_liab"]
        - stock_file["acct_pay"]
        - stock_file["adv_acct"]
        - stock_file["emp_comp_pay"]
        - stock_file["tax_pay"]
        - stock_file["oth_pay"]
        - stock_file["DEFEREVE"]
        - stock_file["oth_cur_liab"]
        + stock_file["lt_ln"]
        + stock_file["bnd_pay"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "financial_liability"]]
    return stock_file


# # 质量类因子


# 净利润与营业总收入之比 net_profit_to_total_operate_revenue_ttm
# =净利润与营业总收入之比=净利润（TTM）net_prof/营业总收入（TTM）ttl_inc_oper
def net_profit_to_total_operate_revenue_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=4).sum().values
    )
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["net_profit_to_total_operate_revenue_ttm"] = (
        stock_file["net_prof_ttm"] / stock_file["ttl_inc_oper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_profit_to_total_operate_revenue_ttm"]
    ]
    return stock_file


# 经营活动产生的现金流量净额与企业价值之比TTM cfo_to_ev
# 经营活动产生的现金流量净额TTM net_cf_oper / 企业价值。其中，企业价值=公司市值TOTMKTCAP+负债合计(ttl_liab)-货币资金(mny_cptl)
# 公司市值（share_total*close）
def cfo_to_ev(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=252).sum().values
    )
    stock_file["company_value"] = (
        stock_file["TOTMKTCAP"] + stock_file["ttl_liab"] - stock_file["mny_cptl"]
    )
    stock_file["cfo_to_ev"] = (
        stock_file["net_cf_oper_ttm"] / stock_file["company_value"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "cfo_to_ev"]]
    return stock_file


# 应付账款周转天数 accounts_payable_turnover_days
# 应付账款周转天数 = 360 / 应付账款周转率(ACCPAYRT)
def accounts_payable_turnover_days(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["accounts_payable_turnover_days"] = 360 / stock_file["ACCPAYRT"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "accounts_payable_turnover_days"]
    ]
    return stock_file


# 销售净利率 net_profit_ratio
# =净利润（TTM）net_prof/营业收入（TTM）inc_oper
def net_profit_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["net_profit_ratio"] = (
        stock_file["net_prof_ttm"] / stock_file["inc_oper_ttm"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_profit_ratio"]]
    return stock_file


# 营业外收支利润净额/利润总额 net_non_operating_income_to_total_profit
# 营业外收支利润净额inc_noper-exp_noper/利润总额ttl_prof
def net_non_operating_income_to_total_profit(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_non_operating_income_to_total_profit"] = (
        stock_file["inc_noper"] - stock_file["exp_noper"]
    ) / stock_file["ttl_prof"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_non_operating_income_to_total_profit"]
    ]
    return stock_file


# 固定资产比率 fixed_asset_ratio
# =(固定资产(fix_ast)+工程物资(const_matl)+在建工程 const_prog)/总资产(ttl_ast)
def fixed_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["fixed_asset_ratio"] = (
        stock_file["fix_ast"] + stock_file["const_matl"] + stock_file["const_prog"]
    ) / stock_file["ttl_ast"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "fixed_asset_ratio"]]
    return stock_file


# 应收账款周转天数 account_receivable_turnover_days
# 应收账款周转天数=360/应收账款周转率(ACCRECGTURNRT)
def account_receivable_turnover_days(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["account_receivable_turnover_days"] = 360 / stock_file["ACCRECGTURNRT"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "account_receivable_turnover_days"]
    ]
    return stock_file


# 毛利率增长 DEGM
# 毛利率增长=(今年毛利率 （TTM）/去年毛利率（TTM）)-1
# 毛利率=（营业收入 inc_oper-营业成本 cost_oper）/营业收入
def DEGM(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["grossMargin"] = (
        stock_file["inc_oper"] - stock_file["cost_oper"]
    ) / stock_file["inc_oper"]
    stock_file["grossMargin_ttm"] = (
        stock_file.groupby(["symbol"])["grossMargin"].rolling(window=4).sum().values
    )
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["grossMargin_ttm_4"] = stock_file.groupby(["symbol"])[
        "grossMargin_ttm"
    ].shift(4)
    stock_file["DEGM"] = (
        stock_file["grossMargin_ttm"] / stock_file["grossMargin_ttm_4"]
    ) - 1
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "DEGM"]]
    return stock_file


# 营业费用与营业总收入之比 sale_expense_to_operating_revenue
# =销售费用（TTM）exp_sell/营业总收入（TTM）ttl_inc_oper
def sale_expense_to_operating_revenue(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["exp_sell_ttm"] = (
        stock_file.groupby(["symbol"])["exp_sell"].rolling(window=4).sum().values
    )
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["sale_expense_to_operating_revenue"] = (
        stock_file["exp_sell_ttm"] / stock_file["ttl_inc_oper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "sale_expense_to_operating_revenue"]
    ]
    return stock_file


# 销售税金率 operating_tax_to_operating_revenue_ratio_ttm
# =营业税金及附加（TTM）biz_tax_sur/营业收入（TTM)inc_oper
def operating_tax_to_operating_revenue_ratio_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["biz_tax_sur_ttm"] = (
        stock_file.groupby(["symbol"])["biz_tax_sur"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["operating_tax_to_operating_revenue_ratio_ttm"] = (
        stock_file["biz_tax_sur_ttm"] / stock_file["inc_oper_ttm"]
    )
    stock_file = stock_file[
        [
            "symbol",
            "rpt_date",
            "pub_date",
            "operating_tax_to_operating_revenue_ratio_ttm",
        ]
    ]
    return stock_file


# 存货周转天数 inventory_turnover_days
# 存货周转天数=360/存货周转率 INVTURNRT
def inventory_turnover_days(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inventory_turnover_days"] = 360 / stock_file["INVTURNRT"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "inventory_turnover_days"]
    ]
    return stock_file


# 营业周期 OperatingCycle
# 应收账款周转天数 ACCRECGTURNDAYS+存货周转天数 INVTURNDAYS
def OperatingCycle(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["OperatingCycle"] = (
        stock_file["ACCRECGTURNDAYS"] + stock_file["INVTURNDAYS"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "OperatingCycle"]]
    return stock_file


# 经营活动产生的现金流量净额与经营活动净收益之比net_operate_cash_flow_to_operate_income
# 经营活动产生的现金流量净额（TTM）net_cf_oper/(营业总收入ttl_inc_oper（TTM）-营业总成本（TTM）ttl_cost_oper)
def net_operate_cash_flow_to_operate_income(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file["ttl_cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_cost_oper"].rolling(window=4).sum().values
    )
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["net_operate_cash_flow_to_operate_income"] = stock_file[
        "net_cf_oper_ttm"
    ] / (stock_file["ttl_inc_oper_ttm"] - stock_file["ttl_cost_oper_ttm"])
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cash_flow_to_operate_income"]
    ]
    return stock_file


# 净利润现金含量 net_operating_cash_flow_coverage
# 经营活动产生的现金流量净额net_cf_oper/归属于母公司所有者的净利润net_prof_pcom
def net_operating_cash_flow_coverage(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_operating_cash_flow_coverage"] = (
        stock_file["net_cf_oper"] / stock_file["net_prof_pcom"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operating_cash_flow_coverage"]
    ]
    return stock_file


# 速动比率 quick_ratio
# 速动比率=(流动资产合计ttl_cur_ast-存货 invt)/ 流动负债合计ttl_cur_liab
def quick_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["quick_ratio"] = (
        stock_file["ttl_cur_ast"] - stock_file["invt"]
    ) / stock_file["ttl_cur_liab"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "quick_ratio"]]
    return stock_file


# 无形资产比率 intangible_asset_ratio
# =(无形资产 intg_ast+研发支出exp_rd+商誉 gw)/总资产 ttl_ast
def intangible_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["intangible_asset_ratio"] = (
        stock_file["intg_ast"] + stock_file["exp_rd"] + stock_file["gw"]
    ) / stock_file["ttl_ast"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "intangible_asset_ratio"]
    ]
    return stock_file


# 市场杠杆 MLEV
# =非流动负债合计 ttl_ncur_liab/(非流动负债合计+总市值(share_total*close))
def MLEV(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["MLEV"] = stock_file["ttl_ncur_liab"] / (
        stock_file["ttl_ncur_liab"] + stock_file["TOTMKTCAP"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MLEV"]]
    return stock_file


# 产权比率 debt_to_equity_ratio
# =负债合计 ttl_liab/归属母公司所有者权益合计 ttl_eqy_pcom
def debt_to_equity_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["debt_to_equity_ratio"] = (
        stock_file["ttl_liab"] / stock_file["ttl_eqy_pcom"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "debt_to_equity_ratio"]]
    return stock_file


# 超速动比率 super_quick_ratio
# （货币资金 mny_cptl+交易性金融资产 trd_fin_ast+应收票据 note_rcv+应收帐款 acct_rcv+其他应收款 oth_rcv）／流动负债合计ttl_cur_liab
def super_quick_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["super_quick_ratio"] = (
        stock_file["mny_cptl"]
        + stock_file["trd_fin_ast"]
        + stock_file["note_rcv"]
        + stock_file["acct_rcv"]
        + stock_file["oth_rcv"]
    ) / stock_file["ttl_cur_liab"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "super_quick_ratio"]]
    return stock_file


# 存货周转率 inventory_turnover_rate
# =营业成本（TTM）cost_oper/存货 invt
def inventory_turnover_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file["inventory_turnover_rate"] = (
        stock_file["cost_oper_ttm"] / stock_file["invt"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "inventory_turnover_rate"]
    ]
    return stock_file


# 营业利润增长率 operating_profit_growth_rate
# =(今年营业利润（TTM）oper_prof/去年营业利润（TTM）)-1
def operating_profit_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["oper_prof_ttm"] = (
        stock_file.groupby(["symbol"])["oper_prof"].rolling(window=4).sum().values
    )
    stock_file["oper_prof_ttm_4"] = stock_file.groupby(["symbol"])[
        "oper_prof_ttm"
    ].shift(4)
    stock_file["operating_profit_growth_rate"] = (
        stock_file["oper_prof_ttm"] / stock_file["oper_prof_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_profit_growth_rate"]
    ]
    return stock_file


# 长期负债与营运资金比率 long_debt_to_working_capital_ratio
# =非流动负债合计 ttl_ncur_liab/(流动资产合计ttl_cur_ast-流动负债合计ttl_cur_liab)
def long_debt_to_working_capital_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["long_debt_to_working_capital_ratio"] = stock_file["ttl_ncur_liab"] / (
        stock_file["ttl_cur_ast"] - stock_file["ttl_cur_liab"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "long_debt_to_working_capital_ratio"]
    ]
    return stock_file


# 流动比率(单季度) current_ratio
# =流动资产合计ttl_cur_ast/流动负债合计ttl_cur_liab
def current_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["current_ratio"] = stock_file["ttl_cur_ast"] / stock_file["ttl_cur_liab"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "current_ratio"]]
    return stock_file


# 经营活动产生现金流量净额/净债务 net_operate_cash_flow_to_net_debt
# 经营活动产生现金流量净额 net_cf_oper/净债务 NDEBT
def net_operate_cash_flow_to_net_debt(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_1"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file["net_operate_cash_flow_to_net_debt"] = (
        stock_file["net_cf_oper_1"] / stock_file["NDEBT"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cash_flow_to_net_debt"]
    ]
    return stock_file


# 总资产现金回收率 net_operate_cash_flow_to_asset
# 经营活动产生的现金流量净额(ttm)net_cf_oper / 总资产 ttl_ast
def net_operate_cash_flow_to_asset(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file["net_operate_cash_flow_to_asset"] = (
        stock_file["net_cf_oper_ttm"] / stock_file["ttl_ast"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cash_flow_to_asset"]
    ]
    return stock_file


# 非流动资产比率 non_current_asset_ratio
# 非流动资产比率=非流动资产合计 ttl_ncur_ast/总资产 ttl_ast
def non_current_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["non_current_asset_ratio"] = (
        stock_file["ttl_ncur_ast"] / stock_file["ttl_ast"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "non_current_asset_ratio"]
    ]
    return stock_file


# 总资产周转率 total_asset_turnover_rate
# =营业收入inc_oper(ttm)/总资产 ttl_ast
def total_asset_turnover_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["total_asset_turnover_rate"] = (
        stock_file["inc_oper_ttm"] / stock_file["ttl_ast"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_asset_turnover_rate"]
    ]
    return stock_file


# 长期借款与资产总计之比 long_debt_to_asset_ratio
# =长期借款 lt_ln/总资产 ttl_ast
def long_debt_to_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["long_debt_to_asset_ratio"] = stock_file["lt_ln"] / stock_file["ttl_ast"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "long_debt_to_asset_ratio"]
    ]
    return stock_file


# 有形净值债务率 debt_to_tangible_equity_ratio
# 负债合计 ttl_liab/有形净值 其中有形净值=股东权益 ttl_eqy-无形资产净值，无形资产净值= 商誉 gw+无形资产 intg_ast
def debt_to_tangible_equity_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["debt_to_tangible_equity_ratio"] = stock_file["ttl_liab"] / (
        stock_file["ttl_eqy"] - (stock_file["gw"] + stock_file["intg_ast"])
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "debt_to_tangible_equity_ratio"]
    ]
    return stock_file


# 总资产报酬率 ROAEBITTTM
# （利润总额ttl_prof（TTM）+利息支出exp_int（TTM）） / 总资产 ttl_ast 在过去12个月的平均
def ROAEBITTTM(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_prof_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_prof"].rolling(window=4).sum().values
    )
    stock_file["exp_int_ttm"] = (
        stock_file.groupby(["symbol"])["exp_int"].rolling(window=4).sum().values
    )
    stock_file["ttl_ast_mean"] = (
        stock_file.groupby(["symbol"])["ttl_ast"].rolling(window=4).mean().values
    )
    stock_file["ROAEBITTTM"] = (
        stock_file["ttl_prof_ttm"] + stock_file["exp_int_ttm"]
    ) / stock_file["ttl_ast_mean"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ROAEBITTTM"]]
    return stock_file


# 营业利润率 operating_profit_ratio
# =营业利润oper_prof（TTM）/营业收入（TTM）inc_oper
def operating_profit_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["oper_prof_ttm"] = (
        stock_file.groupby(["symbol"])["oper_prof"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["operating_profit_ratio"] = (
        stock_file["oper_prof_ttm"] / stock_file["inc_oper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_profit_ratio"]
    ]
    return stock_file


# 长期负债与资产总计之比 long_term_debt_to_asset_ratio
# =非流动负债合计 ttl_ncur_liab/总资产 ttl_ast
def long_term_debt_to_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["long_term_debt_to_asset_ratio"] = (
        stock_file["ttl_ncur_liab"] / stock_file["ttl_ast"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "long_term_debt_to_asset_ratio"]
    ]
    return stock_file


# 流动资产周转率TTM current_asset_turnover_rate
# 过去12个月的营业收入inc_oper/过去12个月的平均流动资产合计ttl_cur_ast
def current_asset_turnover_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["ttl_cur_ast_mean"] = (
        stock_file.groupby(["symbol"])["ttl_cur_ast"].rolling(window=4).mean().values
    )
    stock_file["current_asset_turnover_rate"] = (
        stock_file["inc_oper_ttm"] / stock_file["ttl_cur_ast_mean"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "current_asset_turnover_rate"]
    ]
    return stock_file


# 财务费用与营业总收入之比 financial_expense_rate
# = 财务费用（TTM）fin_exp / 营业总收入（TTM）ttl_inc_oper
def financial_expense_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["fin_exp_ttm"] = (
        stock_file.groupby(["symbol"])["fin_exp"].rolling(window=4).sum().values
    )
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["financial_expense_rate"] = (
        stock_file["fin_exp_ttm"] / stock_file["ttl_inc_oper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "financial_expense_rate"]
    ]
    return stock_file


# 经营活动净收益/利润总额 operating_profit_to_total_profit
# 经营活动净收益/利润总额ttl_prof
# 经营活动净收益=ttl_inc_oper 营业总收入-ttl_cost_oper 营业总成本
def operating_profit_to_total_profit(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["NOPI"] = stock_file["ttl_inc_oper"] - stock_file["ttl_cost_oper"]
    stock_file["operating_profit_to_total_profit"] = (
        stock_file["NOPI"] / stock_file["ttl_prof"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_profit_to_total_profit"]
    ]
    return stock_file


# 债务总资产比 debt_to_asset_ratio
# =负债合计 ttl_liab/总资产 ttl_ast
def debt_to_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["debt_to_asset_ratio"] = stock_file["ttl_liab"] / stock_file["ttl_ast"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "debt_to_asset_ratio"]]
    return stock_file


# 股东权益与固定资产比率 equity_to_fixed_asset_ratio
# =股东权益 ttl_eqy/(固定资产 fix_ast+工程物资 const_matl+在建工程 const_prog)
def equity_to_fixed_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["equity_to_fixed_asset_ratio"] = stock_file["ttl_eqy"] / (
        stock_file["fix_ast"] + stock_file["const_matl"] + stock_file["const_prog"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "equity_to_fixed_asset_ratio"]
    ]
    return stock_file


# 经营活动产生的现金流量净额/负债合计 net_operate_cash_flow_to_total_liability
# 经营活动产生的现金流量净额net_cf_oper/负债合计 ttl_liab
def net_operate_cash_flow_to_total_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_operate_cash_flow_to_total_liability"] = (
        stock_file["net_cf_oper"] / stock_file["ttl_liab"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cash_flow_to_total_liability"]
    ]
    return stock_file


# 经营活动产生的现金流量净额与营业收入之比 cash_rate_of_sales
# 经营活动产生的现金流量净额（TTM） net_cf_oper / 营业收入（TTM）inc_oper
def cash_rate_of_sales(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["cash_rate_of_sales"] = (
        stock_file["net_cf_oper_ttm"] / stock_file["inc_oper_ttm"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "cash_rate_of_sales"]]
    return stock_file


# 营业利润与营业总收入之比 operating_profit_to_operating_revenue
# 营业利润与营业总收入之比=营业利润（TTM） oper_prof/营业总收入（TTM）ttl_inc_oper
def operating_profit_to_operating_revenue(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["oper_prof_ttm"] = (
        stock_file.groupby(["symbol"])["oper_prof"].rolling(window=4).sum().values
    )
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["operating_profit_to_operating_revenue"] = (
        stock_file["oper_prof_ttm"] / stock_file["ttl_inc_oper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_profit_to_operating_revenue"]
    ]
    return stock_file


# 资产回报率TTM roa_ttm
# 资产回报率=净利润 net_prof（TTM）/期末总资产 ttl_ast
def roa_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=4).sum().values
    )
    stock_file["roa_ttm"] = stock_file["net_prof_ttm"] / stock_file["ttl_ast"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "roa_ttm"]]
    return stock_file


# 管理费用与营业总收入之比 admin_expense_rate
# =管理费用（TTM） exp_adm/营业总收入（TTM）ttl_inc_oper
def admin_expense_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["exp_adm_ttm"] = (
        stock_file.groupby(["symbol"])["exp_adm"].rolling(window=4).sum().values
    )
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["admin_expense_rate"] = (
        stock_file["exp_adm_ttm"] / stock_file["ttl_inc_oper_ttm"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "admin_expense_rate"]]
    return stock_file


# 固定资产周转率 fixed_assets_turnover_rate
# 等于过去12个月的营业收入inc_oper/过去12个月的平均（固定资产 fix_ast+工程物资 const_matl+在建工程 const_prog）
def fixed_assets_turnover_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["all"] = (
        stock_file["fix_ast"] + stock_file["const_matl"] + stock_file["const_prog"]
    )
    stock_file["all_mean"] = (
        stock_file.groupby(["symbol"])["all"].rolling(window=4).mean().values
    )
    stock_file["fixed_assets_turnover_rate"] = (
        stock_file["inc_oper_ttm"] / stock_file["all_mean"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "fixed_assets_turnover_rate"]
    ]
    return stock_file


# 对联营和合营公司投资收益/利润总额 invest_income_associates_to_total_profit
# 对联营和营公司投资收益 inv_inv_jv_p/利润总额ttl_prof
def invest_income_associates_to_total_profit(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["invest_income_associates_to_total_profit"] = (
        stock_file["inv_inv_jv_p"] / stock_file["ttl_prof"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "invest_income_associates_to_total_profit"]
    ]
    return stock_file


# 股东权益比率 equity_to_asset_ratio
# 股东权益比率=股东权益 ttl_eqy/总资产 ttl_ast
def equity_to_asset_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["equity_to_asset_ratio"] = stock_file["ttl_eqy"] / stock_file["ttl_ast"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "equity_to_asset_ratio"]]
    return stock_file


# 销售商品提供劳务收到的现金与营业收入之比 goods_service_cash_to_operating_revenue_ttm
# =销售商品和提供劳务收到的现金（TTM）cash_rcv_sale/营业收入inc_oper（TTM）
def goods_service_cash_to_operating_revenue_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cash_rcv_sale_ttm"] = (
        stock_file.groupby(["symbol"])["cash_rcv_sale"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["goods_service_cash_to_operating_revenue_ttm"] = (
        stock_file["cash_rcv_sale_ttm"] / stock_file["inc_oper_ttm"]
    )
    stock_file = stock_file[
        [
            "symbol",
            "rpt_date",
            "pub_date",
            "goods_service_cash_to_operating_revenue_ttm",
        ]
    ]
    return stock_file


# 现金比率 cash_to_current_liability
# 期末现金及现金等价物余额 cash_cash_eq_end/流动负债合计ttl_cur_liab的12个月均值
def cash_to_current_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_cur_liab_mean"] = (
        stock_file.groupby(["symbol"])["ttl_cur_liab"].rolling(window=4).mean().values
    )
    stock_file["cash_to_current_liability"] = (
        stock_file["cash_cash_eq_end"] / stock_file["ttl_cur_liab_mean"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "cash_to_current_liability"]
    ]
    return stock_file


# 现金流动负债比 net_operate_cash_flow_to_total_current_liability
# =经营活动产生的现金流量净额 net_cf_oper（TTM）/流动负债合计 ttl_cur_liab
def net_operate_cash_flow_to_total_current_liability(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file["net_operate_cash_flow_to_total_current_liability"] = (
        stock_file["net_cf_oper_ttm"] / stock_file["ttl_cur_liab"]
    )
    stock_file = stock_file[
        [
            "symbol",
            "rpt_date",
            "pub_date",
            "net_operate_cash_flow_to_total_current_liability",
        ]
    ]
    return stock_file


# 现金流资产比和资产回报率之差 ACCA
# 现金流资产比-资产回报率,其中现金流资产比=经营活动产生的现金流量净额net_cf_oper/总资产 ttl_ast
# 资产回报率=净利润 net_prof/总资产 ttl_ast
def ACCA(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["现金流资产比"] = stock_file["net_cf_oper"] / stock_file["ttl_ast"]
    stock_file["资产回报率"] = stock_file["net_prof"] / stock_file["ttl_ast"]
    stock_file["ACCA"] = stock_file["现金流资产比"] - stock_file["资产回报率"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ACCA"]]
    return stock_file


# 权益回报率TTM roe_ttm
# =净利润 net_prof（TTM）/期末股东权益 ttl_eqy
def roe_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=4).sum().values
    )
    stock_file["roe_ttm"] = stock_file["net_prof_ttm"] / stock_file["ttl_eqy"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "roe_ttm"]]
    return stock_file


# 应付账款周转率 accounts_payable_turnover_rate
# TTM(营业成本 cost_oper,0)/（AvgQ(应付账款 acct_pay,4,0) + AvgQ(应付票据 note_pay,4,0) + AvgQ(预付款项 ppay,4,0) ）
def accounts_payable_turnover_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file["acct_pay_mean"] = (
        stock_file.groupby(["symbol"])["acct_pay"].rolling(window=4).mean().values
    )
    stock_file["note_pay_mean"] = (
        stock_file.groupby(["symbol"])["note_pay"].rolling(window=4).mean().values
    )
    stock_file["ppay_mean"] = (
        stock_file.groupby(["symbol"])["ppay"].rolling(window=4).mean().values
    )
    stock_file["accounts_payable_turnover_rate"] = stock_file["cost_oper_ttm"] / (
        stock_file["acct_pay_mean"]
        + stock_file["note_pay_mean"]
        + stock_file["ppay_mean"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "accounts_payable_turnover_rate"]
    ]
    return stock_file


# 销售毛利率 gross_income_ratio
# =(营业收入inc_oper（TTM）-营业成本 cost_oper（TTM）)/营业收入（TTM）
def gross_income_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file["gross_income_ratio"] = (
        stock_file["inc_oper_ttm"] - stock_file["cost_oper_ttm"]
    ) / stock_file["inc_oper_ttm"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "gross_income_ratio"]]
    return stock_file


# 扣除非经常损益后的净利润/净利润 adjusted_profit_to_total_profit
# 扣除非经常损益后的净利润  NPCUT/净利润 net_prof
def adjusted_profit_to_total_profit(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["adjusted_profit_to_total_profit"] = (
        stock_file["NPCUT"] / stock_file["net_prof"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "adjusted_profit_to_total_profit"]
    ]
    return stock_file


# 应收账款周转率 account_receivable_turnover_rate
# TTM(营业收入,0)inc_oper/（AvgQ(应收账款 acct_rcv,4,0) + AvgQ(应收票据 note_rcv,4,0) + AvgQ(预收账款 acct_rcv_adv,4,0) ）
def account_receivable_turnover_rate(stock_file):
    stock_file = stock_file.fillna(0)
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["acct_rcv_mean"] = (
        stock_file.groupby(["symbol"])["acct_rcv"].rolling(window=4).mean().values
    )
    stock_file["note_rcv_mean"] = (
        stock_file.groupby(["symbol"])["note_rcv"].rolling(window=4).mean().values
    )
    stock_file["adv_acct_mean"] = (
        stock_file.groupby(["symbol"])["acct_rcv_adv"].rolling(window=4).mean().values
    )
    stock_file["account_receivable_turnover_rate"] = stock_file["inc_oper_ttm"] / (
        stock_file["acct_rcv_mean"]
        + stock_file["note_rcv_mean"]
        + stock_file["adv_acct_mean"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "account_receivable_turnover_rate"]
    ]
    return stock_file


# 股东权益周转率 equity_turnover_rate
# =营业收入inc_oper(ttm)/股东权益 ttl_eqy
def equity_turnover_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["equity_turnover_rate"] = (
        stock_file["inc_oper_ttm"] / stock_file["ttl_eqy"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "equity_turnover_rate"]]
    return stock_file


# 成本费用利润率 total_profit_to_cost_ratio
# =利润总额ttl_prof/(营业成本cost_oper+财务费用fin_exp+销售费用exp_sell+管理费用exp_adm)，以上科目使用的都是TTM的数值
def total_profit_to_cost_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_prof_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_prof"].rolling(window=4).sum().values
    )
    stock_file["cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file["fin_exp_ttm"] = (
        stock_file.groupby(["symbol"])["fin_exp"].rolling(window=4).sum().values
    )
    stock_file["exp_sell_ttm"] = (
        stock_file.groupby(["symbol"])["exp_sell"].rolling(window=4).sum().values
    )
    stock_file["exp_adm_ttm"] = (
        stock_file.groupby(["symbol"])["exp_adm"].rolling(window=4).sum().values
    )
    stock_file["total_profit_to_cost_ratio"] = stock_file["ttl_prof_ttm"] / (
        stock_file["cost_oper_ttm"]
        + stock_file["fin_exp_ttm"]
        + stock_file["exp_sell_ttm"]
        + stock_file["exp_adm_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_profit_to_cost_ratio"]
    ]
    return stock_file


# 销售成本率 operating_cost_to_operating_revenue_ratio
# =营业成本（TTM）cost_oper/营业收入（TTM inc_oper
def operating_cost_to_operating_revenue_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["operating_cost_to_operating_revenue_ratio"] = (
        stock_file["cost_oper_ttm"] / stock_file["inc_oper_ttm"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_cost_to_operating_revenue_ratio"]
    ]
    return stock_file


# 财务杠杆指数 LVGI
# 资产负债率=负债总额 ttl_liab/资产总额 ttl_ast
# 本期(年报)资产负债率/上期(年报)资产负债率
def LVGI(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ASSLIABRT"] = stock_file["ttl_liab"] / stock_file["ttl_ast"]
    stock_file["ASSLIABRT_4"] = stock_file.groupby(["symbol"])["ASSLIABRT"].shift(4)
    stock_file["LVGI"] = stock_file["ASSLIABRT"] / stock_file["ASSLIABRT_4"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "LVGI"]]
    stock_file["Q"] = stock_file["rpt_date"].dt.quarter
    stock_file = stock_file[stock_file["Q"] == 4].reset_index(drop=True)
    return stock_file


# 营业收入指数 SGI
# 本期(年报)营业收入/上期(年报)营业收入inc_oper
def SGI(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm_4"] = stock_file.groupby(["symbol"])["inc_oper_ttm"].shift(
        4
    )
    stock_file["SGI"] = stock_file["inc_oper_ttm"] / stock_file["inc_oper_ttm_4"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "SGI"]]
    return stock_file


# 毛利率指数 GMI
# 上期(年报)毛利率/本期(年报)毛利率
# 毛利率=（营业收入 inc_oper-营业成本 cost_oper）/营业收入
def GMI(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    # stock_file['quarter']=df['rpt_date'].dt.quarter
    # stock_file=stock_file[stock_file['quarter']==4]
    # stock_file=stock_file.sort_values(['symbol','rpt_date']).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["cost_oper_ttm"] = (
        stock_file.groupby(["symbol"])["cost_oper"].rolling(window=4).sum().values
    )
    stock_file["grossMargin"] = (
        stock_file["inc_oper_ttm"] - stock_file["cost_oper_ttm"]
    ) / stock_file["inc_oper_ttm"]
    stock_file["grossMargin_y_1"] = stock_file.groupby(["symbol"])["grossMargin"].shift(
        4
    )
    stock_file["GMI"] = stock_file["grossMargin"] / stock_file["grossMargin_y_1"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "GMI"]]
    return stock_file


# 应收账款指数 DSRI
# 本期(年报)应收账款acct_rcv占营业收入inc_oper比例/上期(年报)应收账款占营业收入比例
def DSRI(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["acct_rcv_ttm"] = (
        stock_file.groupby(["symbol"])["acct_rcv"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["ratio"] = stock_file["acct_rcv_ttm"] / stock_file["inc_oper_ttm"]
    stock_file["ratio_4"] = stock_file.groupby(["symbol"])["ratio"].shift(4)
    stock_file["DSRI"] = stock_file["ratio"] / stock_file["ratio_4"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "DSRI"]]
    return stock_file


# 经营资产回报率TTM rnoa_ttm
# 销售利润率*经营资产周转率???


# 销售利润率TTM profit_margin_ttm
# 营业利润 oper_prof/营业收入 inc_oper
def profit_margin_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["profit_margin_ttm"] = stock_file["oper_prof"] / stock_file["inc_oper"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "profit_margin_ttm"]]
    return stock_file


# 长期权益回报率TTM roe_ttm_8y
# 8年(1+roe_ttm)的累乘 ^ (1/8) - 1 # 至少要有近4年的数据，否则为 nan
# roe_ttm=净利润 net_prof（TTM）/期末股东权益 ttl_eqy
def roe_ttm_8y(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["roe_ttm_1"] = (stock_file["net_prof"] / stock_file["ttl_eqy"]) + 1
    stock_file["roe_ttm_1_1"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(1)
    stock_file["roe_ttm_1_2"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(2)
    stock_file["roe_ttm_1_3"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(3)
    stock_file["roe_ttm_1_4"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(4)
    stock_file["roe_ttm_1_5"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(5)
    stock_file["roe_ttm_1_6"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(6)
    stock_file["roe_ttm_1_7"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(7)
    stock_file["roe_ttm_1_8"] = stock_file.groupby(["symbol"])["roe_ttm_1"].shift(8)
    stock_file["roe_ttm_4y"] = (
        stock_file["roe_ttm_1_1"]
        * stock_file["roe_ttm_1_2"]
        * stock_file["roe_ttm_1_3"]
        * stock_file["roe_ttm_1_4"]
    )
    stock_file["roe_ttm_8y"] = (
        stock_file["roe_ttm_1_1"]
        * stock_file["roe_ttm_1_2"]
        * stock_file["roe_ttm_1_3"]
        * stock_file["roe_ttm_1_4"]
        * stock_file["roe_ttm_1_5"]
        * stock_file["roe_ttm_1_6"]
        * stock_file["roe_ttm_1_7"]
        * stock_file["roe_ttm_1_8"]
    )
    stock_file["roe_ttm_8y"] = stock_file["roe_ttm_8y"] ** (1 / 8) - 1
    stock_file["roe_ttm_4y"] = stock_file["roe_ttm_4y"] ** (1 / 4) - 1
    list1 = list(stock_file[stock_file["roe_ttm_8y"] == np.nan].index)
    stock_file.loc[list1, "roe_ttm_8y"] = stock_file.loc[list1, "roe_ttm_4y"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "roe_ttm_8y"]]
    return stock_file


# 经营资产周转率TTM asset_turnover_ttm？？？
# 营业收入TTM inc_oper/近4个季度期末净经营性资产均值; 净经营性资产=经营资产-经营负债？？？


# 投资资本回报率TTM roic_ttm
# 权益回报率=归属于母公司股东的净利润 net_prof_pcom（TTM）/ 前四个季度投资资本均值;
# 投资资本=股东权益 ttl_eqy+负债合计 ttl_liab-无息流动负债 -无息非流动负债 ;
# 无息流动负债=应付账款 acct_pay+预收款项 adv_acct+应付职工薪酬 emp_comp_pay+应交税费 tax_pay+其他应付款 oth_pay+一年内的递延收益 DEFEREVE+其它流动负债 oth_cur_liab;
# 无息非流动负债=非流动负债合计 ttl_ncur_liab-长期借款 lt_ln-应付债券 bnd_pay；
def roic_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_pcom_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof_pcom"].rolling(window=4).sum().values
    )
    stock_file["all"] = (
        stock_file["ttl_eqy"]
        + stock_file["ttl_liab"]
        - (
            stock_file["acct_pay"]
            + stock_file["adv_acct"]
            + stock_file["emp_comp_pay"]
            + stock_file["tax_pay"]
            + stock_file["oth_pay"]
            + stock_file["DEFEREVE"]
            + stock_file["oth_cur_liab"]
        )
        - (stock_file["ttl_ncur_liab"] - stock_file["lt_ln"] - stock_file["bnd_pay"])
    )
    stock_file["all_mean"] = (
        stock_file.groupby(["symbol"])["all"].rolling(window=4).mean().values
    )
    stock_file["roic_ttm"] = stock_file["net_prof_pcom_ttm"] / stock_file["all_mean"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "roic_ttm"]]
    return stock_file


# 长期资产回报率TTM roa_ttm_8y？？？
# 8年(1+roa_ttm)的乘积 ^ (1/8) - 1 # 至少要有近4年的数据，否则为 nan
# roa_ttm资产回报率=净利润net_prof（TTM）/期末总资产 ttl_ast
def roa_ttm_8y(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["roa_ttm_1"] = (stock_file["net_prof"] / stock_file["ttl_ast"]) + 1
    stock_file["roa_ttm_1_1"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(1)
    stock_file["roa_ttm_1_2"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(2)
    stock_file["roa_ttm_1_3"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(3)
    stock_file["roa_ttm_1_4"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(4)
    stock_file["roa_ttm_1_5"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(5)
    stock_file["roa_ttm_1_6"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(6)
    stock_file["roa_ttm_1_7"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(7)
    stock_file["roa_ttm_1_8"] = stock_file.groupby(["symbol"])["roa_ttm_1"].shift(8)
    stock_file["roa_ttm_4y"] = (
        stock_file["roa_ttm_1_1"]
        * stock_file["roa_ttm_1_2"]
        * stock_file["roa_ttm_1_3"]
        * stock_file["roa_ttm_1_4"]
    )
    stock_file["roa_ttm_8y"] = (
        stock_file["roa_ttm_1_1"]
        * stock_file["roa_ttm_1_2"]
        * stock_file["roa_ttm_1_3"]
        * stock_file["roa_ttm_1_4"]
        * stock_file["roa_ttm_1_5"]
        * stock_file["roa_ttm_1_6"]
        * stock_file["roa_ttm_1_7"]
        * stock_file["roa_ttm_1_8"]
    )
    stock_file["roa_ttm_8y"] = stock_file["roa_ttm_8y"] ** (1 / 8) - 1
    stock_file["roa_ttm_4y"] = stock_file["roa_ttm_4y"] ** (1 / 4) - 1
    list1 = list(stock_file[stock_file["roa_ttm_8y"] == np.nan].index)
    stock_file.loc[list1, "roa_ttm_8y"] = stock_file.loc[list1, "roa_ttm_4y"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "roa_ttm_8y"]]
    return stock_file


# 销售管理费用指数 SGAI
# 本期(年报)销售管理费用exp_sell+exp_adm占营业收入 inc_oper 的比例/上期(年报)销售管理费用占营业收入的比例
def SGAI(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["a"] = stock_file["exp_sell"] + stock_file["exp_adm"]
    stock_file["a_ttm"] = (
        stock_file.groupby(["symbol"])["a"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["a_"] = stock_file["a_ttm"] / stock_file["inc_oper_ttm"]
    stock_file["a_4_shift"] = stock_file.groupby(["symbol"])["a_"].shift(4)
    stock_file["SGAI"] = stock_file["a_"] / stock_file["a_4_shift"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "SGAI"]]
    stock_file["Q"] = stock_file["rpt_date"].dt.quarter
    stock_file = stock_file[stock_file["Q"] == 4].reset_index(drop=True)
    return stock_file


# 长期毛利率增长 DEGM_8y
# 过去8年(1+DEGM)的累成 ^ (1/8) - 1
##毛利率=（营业收入 inc_oper-营业成本 cost_oper）/营业收入
def DEGM_8y(stock_file):
    stock_file = stock_file.copy()
    stock_file = DEGM(stock_file)
    # 将数据转换为年度数据？？
    # stock_file['quarter']=df['rpt_date'].dt.quarter
    # stock_file=stock_file[stock_file['quarter']==4]
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["DEGM"] = stock_file["DEGM"] + 1
    stock_file["DEGM_1"] = stock_file.groupby(["symbol"])["DEGM"].shift(4)
    stock_file["DEGM_2"] = stock_file.groupby(["symbol"])["DEGM_1"].shift(4)
    stock_file["DEGM_3"] = stock_file.groupby(["symbol"])["DEGM_2"].shift(4)
    stock_file["DEGM_4"] = stock_file.groupby(["symbol"])["DEGM_3"].shift(4)
    stock_file["DEGM_5"] = stock_file.groupby(["symbol"])["DEGM_4"].shift(4)
    stock_file["DEGM_6"] = stock_file.groupby(["symbol"])["DEGM_5"].shift(4)
    stock_file["DEGM_7"] = stock_file.groupby(["symbol"])["DEGM_6"].shift(4)
    stock_file["DEGM_8"] = stock_file.groupby(["symbol"])["DEGM_7"].shift(4)

    stock_file["DEGM_8y"] = (
        stock_file["DEGM_1"]
        * stock_file["DEGM_2"]
        * stock_file["DEGM_3"]
        * stock_file["DEGM_4"]
        * stock_file["DEGM_5"]
        * stock_file["DEGM_6"]
        * stock_file["DEGM_7"]
        * stock_file["DEGM_8"]
    ) ** (1 / 8) - 1
    stock_file["DEGM_8y"] = (
        stock_file["DEGM_1"]
        * stock_file["DEGM_2"]
        * stock_file["DEGM_3"]
        * stock_file["DEGM_4"]
    ) ** (1 / 4) - 1
    for i in stock_file.index:
        if stock_file.loc[i, "DEGM_8y"] == np.nan:
            stock_file.loc[i, "DEGM_8y"] = stock_file.loc[i, "DEGM_4y"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "DEGM_8y"]]
    return stock_file


# 最大盈利水平 maximum_margin
# max(margin_stability, DEGM_8y)
def maximum_margin(stock_file):
    stock_file = stock_file.copy()
    stock_file1 = DEGM_8y(stock_file)
    stock_file2 = margin_stability(stock_file)
    stock_file = pd.merge(
        stock_file1, stock_file2, on=["symbol", "rpt_date", "pub_date"]
    )
    stock_file["maximum_margin"] = stock_file[["DEGM_8y", "margin_stability"]].max(
        axis=1
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "maximum_margin"]]
    return stock_file


# 盈利能力稳定性 margin_stability
# mean(GM)/std(GM); GM 为过去8年毛利率（grossMargin）ttm
##毛利率=（营业收入 inc_oper-营业成本 cost_oper）/营业收入
def margin_stability(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["grossMargin"] = (
        stock_file["inc_oper"] - stock_file["cost_oper"]
    ) / stock_file["inc_oper"]
    stock_file["grossMargin_ttm"] = (
        stock_file.groupby(["symbol"])["grossMargin"].rolling(window=4).sum().values
    )
    stock_file["grossMargin_ttm_mean8"] = (
        stock_file.groupby(["symbol"])["grossMargin_ttm"]
        .rolling(window=32)
        .mean()
        .values
    )
    stock_file["grossMargin_ttm_std8"] = (
        stock_file.groupby(["symbol"])["grossMargin_ttm"]
        .rolling(window=32)
        .std()
        .values
    )
    stock_file["margin_stability"] = (
        stock_file["grossMargin_ttm_mean8"] / stock_file["grossMargin_ttm_std8"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "margin_stability"]]
    return stock_file


# # 每股指标因子


# 每股营业总收入TTM total_operating_revenue_per_share_ttm
# 营业总收入 ttl_inc_oper（TTM）除以总股本 share_total
def total_operating_revenue_per_share_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_inc_oper"].rolling(window=4).sum().values
    )
    stock_file["total_operating_revenue_per_share_ttm"] = (
        stock_file["ttl_inc_oper_ttm"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_operating_revenue_per_share_ttm"]
    ]
    return stock_file


# 每股现金及现金等价物余额 cash_and_equivalents_per_share
# 每股现金及现金等价物余额 cash_cash_eq_end  share_total
def cash_and_equivalents_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cash_and_equivalents_per_share"] = (
        stock_file["cash_cash_eq_end"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "cash_and_equivalents_per_share"]
    ]
    return stock_file


# 每股盈余公积金 surplus_reserve_fund_per_share
# 盈余公积金 sur_rsv/share_total
def surplus_reserve_fund_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["surplus_reserve_fund_per_share"] = (
        stock_file["sur_rsv"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "surplus_reserve_fund_per_share"]
    ]
    return stock_file


# 每股未分配利润 retained_profit_per_share
# 每股未分配利润 ret_prof share_total
def retained_profit_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["retained_profit_per_share"] = (
        stock_file["ret_prof"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "retained_profit_per_share"]
    ]
    return stock_file


# 每股营业收入TTM operating_revenue_per_share_ttm
# 营业收入 inc_oper（TTM）除以总股本 share_total
def operating_revenue_per_share_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["operating_revenue_per_share_ttm"] = (
        stock_file["inc_oper_ttm"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_revenue_per_share_ttm"]
    ]
    return stock_file


# 每股净资产 net_asset_per_share
# (归属母公司所有者权益合计ttl_eqy_pcom-其他权益工具 oth_eqy)除以总股本
def net_asset_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_asset_per_share"] = (
        stock_file["ttl_eqy_pcom"] - stock_file["oth_eqy"]
    ) / stock_file["share_total"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_asset_per_share"]]
    return stock_file


# 每股营业总收入 total_operating_revenue_per_share
# 营业总收入 ttl_inc_oper share_total
def total_operating_revenue_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["total_operating_revenue_per_share"] = (
        stock_file["ttl_inc_oper"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_operating_revenue_per_share"]
    ]
    return stock_file


# 每股留存收益 retained_earnings_per_share
# 每股留存收益 留存收益=(盈余公积 sur_rsv＋未分配利润 ret_prof)
def retained_earnings_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["retained_earnings_per_share"] = (
        stock_file["sur_rsv"] + stock_file["ret_prof"]
    ) / stock_file["share_total"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "retained_earnings_per_share"]
    ]
    return stock_file


# 每股营业收入 operating_revenue_per_share
# 营业收入 inc_oper
def operating_revenue_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["operating_revenue_per_share"] = (
        stock_file["inc_oper"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_revenue_per_share"]
    ]
    return stock_file


# 每股经营活动产生的现金流量净额 net_operate_cash_flow_per_share
# 经营活动产生的现金流量净额 net_cf_oper
def net_operate_cash_flow_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_operate_cash_flow_per_share"] = (
        stock_file["net_cf_oper"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cash_flow_per_share"]
    ]
    return stock_file


# 每股营业利润TTM operating_profit_per_share_ttm
# 营业利润 oper_prof（TTM）除以总股本 share_total
def operating_profit_per_share_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["oper_prof_ttm"] = (
        stock_file.groupby(["symbol"])["oper_prof"].rolling(window=4).sum().values
    )
    stock_file["operating_profit_per_share_ttm"] = (
        stock_file["oper_prof_ttm"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_profit_per_share_ttm"]
    ]
    return stock_file


# 每股收益TTM eps_ttm
# 过去12个月归属母公司所有者的净利润 net_prof_pcom（TTM）除以总股本 share_total
def eps_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_pcom_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof_pcom"].rolling(window=4).sum().values
    )
    stock_file["eps_ttm"] = stock_file["net_prof_pcom_ttm"] / stock_file["share_total"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "eps_ttm"]]
    return stock_file


# 每股现金流量净额，根据当时日期来获取最近变更日的总股本 cashflow_per_share_ttm
# 现金流量净额（TTM）除以总股本
# 现金流量净额 net_incr_cash_eq
def cashflow_per_share_ttm(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cashflow_per_share_ttm"] = (
        stock_file["net_incr_cash_eq"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "cashflow_per_share_ttm"]
    ]
    return stock_file


# 每股营业利润 operating_profit_per_share
# 每股营业利润oper_prof 总股本 share_total
def operating_profit_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["operating_profit_per_share"] = (
        stock_file["oper_prof"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_profit_per_share"]
    ]
    return stock_file


# 每股资本公积金 capital_reserve_fund_per_share
# 资本公积 cptl_rsv
def capital_reserve_fund_per_share(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["capital_reserve_fund_per_share"] = (
        stock_file["cptl_rsv"] / stock_file["share_total"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "capital_reserve_fund_per_share"]
    ]
    return stock_file


# # 情绪类因子


# 心理线指标 PSY
# n日内连续上涨的天数/n *100。 本因子的计算窗口为12日。
def PSY(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    up = []
    stock_file.loc[
        stock_file[stock_file["open"] <= stock_file["close"]].index, "up"
    ] = 1
    stock_file["up"] = stock_file["up"].fillna(0)
    stock_file["up_day"] = (
        stock_file.groupby(["symbol"])["up"].rolling(window=12).sum().values
    )
    stock_file["PSY"] = stock_file["up_day"] * 100 / 12
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "PSY"]]
    return stock_file


# 12日量变动速率指标 VROC12
# 成交量减N日前的成交量，再除以N日前的成交量，放大100倍，得到VROC值 ，n=12
def VROC12(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["volume_12"] = stock_file.groupby(["symbol"])["volume"].shift(12)
    stock_file["VROC12"] = (
        (stock_file["volume"] - stock_file["volume_12"]) * 100 / stock_file["volume_12"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VROC12"]]
    return stock_file


# 6日成交金额的移动平均值 TVMA6
# 6日成交金额的移动平均值
def TVMA6(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TVMA6"] = (
        stock_file.groupby(["symbol"])["amount"].rolling(window=6).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "TVMA6"]]
    return stock_file


# 成交量的10日指数移动平均 VEMA10
def VEMA10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VEMA10"] = (
        stock_file.groupby(["symbol"])["volume"]
        .ewm(adjust=False, span=10)
        .mean()
        .values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VEMA10"]]
    return stock_file


# 成交量比率（Volume Ratio） VR
# VR=（AVS+1/2CVS）/（BVS+1/2CVS）
# def VR1(data):
#     data=data.copy()
#     if len(data)>=n:#???
#         up=data[data['a']>0]
#         av=up['volume'].sum()
#         avs=2*av
#         down=data[data['a']<0]
#         bv=down['volume'].sum()
#         bvs=2*bv
#         level=data[data['a']==0]
#         cv=level['volume'].sum()
#         cvs=2*cv
#         return [(avs+(1/2)*cvs)/(bvs+(1/2)*cvs)]
#     else:
#         return [np.nan]


# def VR(stock_file):
#     stock_file=stock_file.copy()
#     stock_file=stock_file.sort_values(['symbol','rpt_date']).reset_index(drop=True)
#     stock_file['a']=stock_file['close']-stock_file['pre_close']
#     a=pd.DataFrame((VR1(x) for x in stock_file.groupby(['symbol']).rolling(n)))
#     stock_file['VR']=list(a[0])
#     stock_file=stock_file[['symbol','rpt_date','pub_date','VR']]
#     return stock_file


# 5日平均换手率 VOL5
# 5日换手率的均值,单位为%
def VOL5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL5"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=5).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOL5"]]
    return stock_file


# 意愿指标 BR
# BR=N日内（当日最高价－昨日收盘价）之和 / N日内（昨日收盘价－当日最低价）之和×100 n设定为26
def BR(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["high_preclose"] = stock_file["high"] - stock_file["pre_close"]
    stock_file["preclose_low"] = stock_file["pre_close"] - stock_file["low"]
    stock_file["BR_1"] = (
        stock_file.groupby(["symbol"])["high_preclose"]
        .shift(1)
        .rolling(window=26)
        .sum()
        .values
    )
    stock_file["BR_2"] = (
        stock_file.groupby(["symbol"])["preclose_low"]
        .shift(1)
        .rolling(window=26)
        .sum()
        .values
    )
    stock_file["BR"] = stock_file["BR_1"] / stock_file["BR_2"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "BR"]]
    return stock_file


# 12日成交量的移动平均值 VEMA12
def VEMA12(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VEMA12"] = (
        stock_file.groupby(["symbol"])["volume"].ewm(span=12).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VEMA12"]]
    return stock_file


# 20日成交金额的移动平均值 TVMA20
# 20日成交金额的移动平均值
def TVMA20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TVMA20"] = (
        stock_file.groupby(["symbol"])["amount"].rolling(window=20).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "TVMA20"]]
    return stock_file


# 5日平均换手率与120日平均换手率 DAVOL5
# 5日平均换手率 / 120日平均换手率
def DAVOL5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL5"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=5).mean().values
    )
    stock_file["VOL120"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"]
        .rolling(window=120)
        .mean()
        .values
    )
    stock_file["DAVOL5"] = stock_file["VOL5"] / stock_file["VOL120"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "DAVOL5"]]
    return stock_file


# 计算VMACD因子的中间变量 VDIFF
# EMA(VOLUME，SHORT)-EMA(VOLUME，LONG) short设置为12，long设置为26，M设置为9
def VDIFF(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA12"] = (
        stock_file.groupby(["symbol"])["volume"]
        .ewm(adjust=False, span=12)
        .mean()
        .values
    )
    stock_file["EMA26"] = (
        stock_file.groupby(["symbol"])["volume"]
        .ewm(adjust=False, span=26)
        .mean()
        .values
    )
    stock_file["VDIFF"] = stock_file["EMA12"] - stock_file["EMA26"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VDIFF"]]
    return stock_file


# 威廉变异离散量 WVAD
# (收盘价－开盘价)/(最高价－最低价)×成交量，再做加和，使用过去6个交易日的数据
def WVAD(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["a"] = (
        (stock_file["close"] - stock_file["open"])
        * stock_file["volume"]
        / (stock_file["high"] - stock_file["low"])
    )
    stock_file["WVAD"] = (
        stock_file.groupby(["symbol"])["a"].rolling(window=6).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "WVAD"]]
    return stock_file


# 因子WVAD的6日均值 MAWVAD
# 威廉变异离散量 WVAD=(收盘价－开盘价)/(最高价－最低价)×成交量，再做加和，使用过去6个交易日的数据
def MAWVAD(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["a"] = (
        (stock_file["close"] - stock_file["open"])
        * stock_file["volume"]
        / (stock_file["high"] - stock_file["low"])
    )
    stock_file["WVAD"] = (
        stock_file.groupby(["symbol"])["a"].rolling(window=6).sum().values
    )
    stock_file["MAWVAD"] = (
        stock_file.groupby(["symbol"])["WVAD"].rolling(window=6).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MAWVAD"]]
    return stock_file


# 10日成交量标准差 VSTD10
# 10日成交量标准差
def VSTD10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VSTD10"] = (
        stock_file.groupby(["symbol"])["volume"].rolling(window=10).std().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VSTD10"]]
    return stock_file


# 14日均幅指标 ATR14
# 真实振幅的14日移动平均
def ATR14(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ATR"] = (stock_file["high"] - stock_file["low"]) / stock_file["close"]
    stock_file["ATR14"] = (
        stock_file.groupby(["symbol"])["ATR"].rolling(window=14).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ATR14"]]
    return stock_file


# 10日平均换手率 VOL10
# 10日换手率的均值,单位为%
def VOL10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL10"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=10).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOL10"]]
    return stock_file


# 10日平均换手率与120日平均换手率之比 DAVOL10
# 10日平均换手率 / 120日平均换手率
def DAVOL10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL10"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=10).mean().values
    )
    stock_file["VOL120"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"]
        .rolling(window=120)
        .mean()
        .values
    )
    stock_file["DAVOL10"] = stock_file["VOL10"] / stock_file["VOL120"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "DAVOL10"]]
    return stock_file


# 计算VMACD因子的中间变量 VDEA
# EMA(VDIFF，M) short设置为12，long设置为26，M设置为9
def VDEA(stock_file):
    stock_file = stock_file.copy()
    stock_file = VDIFF(stock_file)
    stock_file["VDEA"] = (
        stock_file.groupby(["symbol"])["VDIFF"].ewm(adjust=False, span=9).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VDEA"]]
    return stock_file


# 20日成交量标准差 VSTD20
# 20日成交量标准差
def VSTD20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VSTD20"] = (
        stock_file.groupby(["symbol"])["volume"].rolling(window=20).std().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VSTD20"]]
    return stock_file


# 6日均幅指标 ATR6
# 真实振幅的6日移动平均
def ATR6(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ATR"] = (stock_file["high"] - stock_file["low"]) / stock_file["close"]
    stock_file["ATR6"] = (
        stock_file.groupby(["symbol"])["ATR"].rolling(window=6).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ATR6"]]
    return stock_file


# 20日平均换手率 VOL20
# 20日换手率的均值,单位为%
def VOL20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL20"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=20).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOL20"]]
    return stock_file


# 20日平均换手率与120日平均换手率之比 DAVOL20
# 20日平均换手率 / 120日平均换手率
def DAVOL20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL20"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=20).mean().values
    )
    stock_file["VOL120"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"]
        .rolling(window=120)
        .mean()
        .values
    )
    stock_file["DAVOL20"] = stock_file["VOL20"] / stock_file["VOL120"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "DAVOL20"]]
    return stock_file


# 成交量指数平滑异同移动平均线 VMACD
# 快的指数移动平均线（EMA12）减去慢的指数移动平均线（EMA26）得到快线DIFF, 由DIFF的M日移动平均得到DEA，由DIFF-DEA的值得到MACD
def VMACD(stock_file):
    stock_file = stock_file.copy()
    stock_file1 = VDIFF(stock_file)
    stock_file2 = VDEA(stock_file)
    stock_file = pd.merge(
        stock_file1, stock_file2, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file["VMACD"] = stock_file["VDIFF"] - stock_file["VDEA"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VMACD"]]
    return stock_file


# 人气指标 AR
# AR=N日内（当日最高价—当日开市价）之和 / N日内（当日开市价—当日最低价）之和 * 100，n设定为26
def AR(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["high_open"] = stock_file["high"] - stock_file["open"]
    stock_file["open_low"] = stock_file["open"] - stock_file["low"]
    stock_file["high_open_26"] = (
        stock_file.groupby(["symbol"])["high_open"].rolling(window=20).sum().values
    )
    stock_file["open_low_26"] = (
        stock_file.groupby(["symbol"])["open_low"].rolling(window=120).sum().values
    )
    stock_file["AR"] = stock_file["high_open_26"] * 100 / stock_file["open_low_26"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "AR"]]
    return stock_file


# 60日平均换手率 VOL60
# 60日换手率的均值,单位为%
def VOL60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL60"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=60).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOL60"]]
    return stock_file


# 换手率相对波动率 turnover_volatility
# 取20个交易日个股换手率的标准差
def turnover_volatility(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["turnover_volatility"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=20).std().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "turnover_volatility"]]
    return stock_file


# 120日平均换手率 VOL120
def VOL120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL120"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"]
        .rolling(window=120)
        .mean()
        .values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOL120"]]
    return stock_file


# 6日量变动速率指标 VROC6
# 成交量减N日前的成交量，再除以N日前的成交量，放大100倍，得到VROC值 ，n=6
def VROC6(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["volume_n"] = stock_file.groupby(["symbol"])["volume"].shift(6)
    stock_file["VROC6"] = (
        (stock_file["volume"] - stock_file["volume_n"]) * 100 / stock_file["volume_n"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VROC6"]]
    return stock_file


# 20日成交金额的标准差 TVSTD20
# 20日成交额的标准差
def TVSTD20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TVSTD20"] = (
        stock_file.groupby(["symbol"])["amount"].rolling(window=20).std().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "TVSTD20"]]
    return stock_file


# ARBR ARBR
# 因子 AR 与因子 BR 的差
def ARBR(stock_file):
    stock_file = stock_file.copy()
    stock_file_AR = AR(stock_file)
    stock_file_BR = BR(stock_file)
    stock_file_AR_BR = pd.merge(
        stock_file_AR, stock_file_BR, on=["symbol", "rpt_date", "pub_date"]
    )
    stock_file_AR_BR["ARBR"] = stock_file_AR_BR["AR"] - stock_file_AR_BR["BR"]
    stock_file_AR_BR = stock_file_AR_BR[["symbol", "rpt_date", "pub_date", "ARBR"]]
    return stock_file_AR_BR


# 20日资金流量 money_flow_20
# 用收盘价、最高价及最低价的均值乘以当日成交量即可得到该交易日的资金流量
def money_flow_20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["money_flow_20_"] = (
        (stock_file["close"] + stock_file["high"] + stock_file["low"])
        * stock_file["volume"]
        / 3
    )
    stock_file["money_flow_20"] = (
        stock_file.groupby(["symbol"])["money_flow_20_"].rolling(window=20).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "money_flow_20"]]
    return stock_file


# 成交量的5日指数移动平均 VEMA5
def VEMA5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VEMA5"] = (
        stock_file.groupby(["symbol"])["volume"].ewm(adjust=False, span=5).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VEMA5"]]
    return stock_file


# 240日平均换手率 VOL240
# 240日换手率的均值,单位为%
def VOL240(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["VOL240"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"]
        .rolling(window=240)
        .mean()
        .values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOL240"]]
    return stock_file


# 成交量的26日指数移动平均 VEMA26
def VEMA26(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VEMA26"] = (
        stock_file.groupby(["symbol"])["volume"]
        .ewm(adjust=False, span=26)
        .mean()
        .values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VEMA26"]]
    return stock_file


# 成交量震荡 VOSC
# 'VEMA12'和'VEMA26'两者的差值，再求差值与'VEMA12'的比，最后将比值放大100倍，得到VOSC值
def VOSC(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["VEMA12"] = (
        stock_file.groupby(["symbol"])["volume"].ewm(span=12).mean().values
    )
    stock_file["VEMA26"] = (
        stock_file.groupby(["symbol"])["volume"].ewm(span=26).mean().values
    )
    stock_file["VOSC"] = (
        (stock_file["VEMA12"] - stock_file["VEMA26"]) * 100 / stock_file["VEMA12"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "VOSC"]]
    return stock_file


# 6日成交金额的标准差 TVSTD6
# 6日成交额的标准差
def TVSTD6(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TVSTD6"] = (
        stock_file.groupby(["symbol"])["amount"].rolling(window=6).std().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "TVSTD6"]]
    return stock_file


# # 成长类因子


# 营业收入增长率 operating_revenue_growth_rate
# 营业收入增长率=（今年营业收入 inc_oper（TTM）/去年营业收入（TTM））-1
def operating_revenue_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["inc_oper_ttm"] = (
        stock_file.groupby(["symbol"])["inc_oper"].rolling(window=4).sum().values
    )
    stock_file["inc_oper_ttm_4"] = stock_file.groupby(["symbol"])["inc_oper_ttm"].shift(
        4
    )
    stock_file["operating_revenue_growth_rate"] = (
        stock_file["inc_oper_ttm"] / stock_file["inc_oper_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "operating_revenue_growth_rate"]
    ]
    return stock_file


# 总资产增长率 total_asset_growth_rate
# 总资产 ttl_ast / 总资产_4 -1
def total_asset_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_ast_4"] = stock_file.groupby(["symbol"])["ttl_ast"].shift(4)
    stock_file["total_asset_growth_rate"] = (
        stock_file["ttl_ast"] / stock_file["ttl_ast_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_asset_growth_rate"]
    ]
    return stock_file


# 经营活动产生的现金流量净额增长率 net_operate_cashflow_growth_rate
# =(今年经营活动产生的现金流量净额 net_cf_oper（TTM）/去年经营活动产生的现金流量净额（TTM）)-1
def net_operate_cashflow_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_oper_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=4).sum().values
    )
    stock_file["net_cf_oper_ttm_4"] = stock_file.groupby(["symbol"])[
        "net_cf_oper_ttm"
    ].shift(4)
    stock_file["net_operate_cashflow_growth_rate"] = (
        stock_file["net_cf_oper_ttm"] / stock_file["net_cf_oper_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_operate_cashflow_growth_rate"]
    ]
    return stock_file


# 利润总额增长率 total_profit_growth_rate
# 利润总额增长率=(今年利润总额 ttl_prof（TTM）/去年利润总额（TTM）)-1
def total_profit_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_prof_ttm"] = (
        stock_file.groupby(["symbol"])["ttl_prof"].rolling(window=4).sum().values
    )
    stock_file["ttl_prof_ttm_4"] = stock_file.groupby(["symbol"])["ttl_prof_ttm"].shift(
        4
    )
    stock_file["total_profit_growth_rate"] = (
        stock_file["ttl_prof_ttm"] / stock_file["ttl_prof_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "total_profit_growth_rate"]
    ]
    return stock_file


# 归属母公司股东的净利润增长率 np_parent_company_owners_growth_rate
# (今年归属于母公司所有者的净利润 net_prof_pcom（TTM）/去年归属于母公司所有者的净利润（TTM）)-1
def np_parent_company_owners_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_pcom_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof_pcom"].rolling(window=4).sum().values
    )
    stock_file["net_prof_pcom_ttm_4"] = stock_file.groupby(["symbol"])[
        "net_prof_pcom_ttm"
    ].shift(4)
    stock_file["np_parent_company_owners_growth_rate"] = (
        stock_file["net_prof_pcom_ttm"] / stock_file["net_prof_pcom_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "np_parent_company_owners_growth_rate"]
    ]
    return stock_file


# 筹资活动产生的现金流量净额增长率 financing_cash_growth_rate
# 过去12个月的筹资现金流量净额 net_cf_fin / 4季度前的12个月的筹资现金流量净额 - 1
def financing_cash_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_cf_fin_ttm"] = (
        stock_file.groupby(["symbol"])["net_cf_fin"].rolling(window=4).sum().values
    )
    stock_file["net_cf_fin_ttm_4"] = stock_file.groupby(["symbol"])[
        "net_cf_fin_ttm"
    ].shift(4)
    stock_file["financing_cash_growth_rate"] = (
        stock_file["net_cf_fin_ttm"] / stock_file["net_cf_fin_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "financing_cash_growth_rate"]
    ]
    return stock_file


# 净利润增长率 net_profit_growth_rate
# 净利润增长率=(今年净利润 net_prof（TTM）/去年净利润（TTM）)-1
def net_profit_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=4).sum().values
    )
    stock_file["net_prof_ttm_4"] = stock_file.groupby(["symbol"])["net_prof_ttm"].shift(
        4
    )
    stock_file["net_profit_growth_rate"] = (
        stock_file["net_prof_ttm"] / stock_file["net_prof_ttm_4"]
    ) - 1
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "net_profit_growth_rate"]
    ]
    return stock_file


# 净资产增长率 net_asset_growth_rate
# （当季的股东权益 ttl_eqy/三季度前的股东权益）-1
def net_asset_growth_rate(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ttl_eqy_1"] = stock_file.groupby(["symbol"])["ttl_eqy"].shift(1)
    stock_file["ttl_eqy_q"] = stock_file["ttl_eqy"] - stock_file["ttl_eqy_1"]
    stock_file["q"] = stock_file["rpt_date"].dt.quarter
    stock_file.loc[
        stock_file[stock_file["q"] == 1].index, "ttl_eqy_q"
    ] = stock_file.loc[stock_file[stock_file["q"] == 1].index, "ttl_eqy"]
    stock_file["ttl_eqy_q_4"] = stock_file.groupby(["symbol"])["ttl_eqy_q"].shift(4)
    stock_file["net_asset_growth_rate"] = (
        stock_file["ttl_eqy_q"] / stock_file["ttl_eqy_q_4"]
    ) - 1
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "net_asset_growth_rate"]]
    return stock_file


# PEG PEG
# PEG = PE  PELFY/ (归母公司净利润net_prof_pcom(TTM)增长率 * 100) # 如果 PE 或 增长率为负，则为 nan
def PEG(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_pcom_ttm"] = (
        stock_file.groupby(["symbol"])["net_prof_pcom"].rolling(window=4).sum().values
    )
    stock_file["net_prof_pcom_ttm_4"] = stock_file.groupby(["symbol"])[
        "net_prof_pcom_ttm"
    ].shift(4)
    stock_file["net_prof_pcom_increase_ratio"] = (
        stock_file["net_prof_pcom_ttm"] / stock_file["net_prof_pcom_ttm_4"]
    ) - 1
    stock_file["PEG"] = stock_file["PELFY"] / (
        stock_file["net_prof_pcom_increase_ratio"] * 100
    )
    list1 = list(
        stock_file[
            (stock_file["PELFY"] < 0) | (stock_file["net_prof_pcom_increase_ratio"] < 0)
        ].index
    )
    stock_file.loc[list1, "PEG"] = np.nan
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "PEG"]]
    return stock_file


# # 风险类因子


# 120日收益方差 Variance120
# 取121个交易日的收盘价 TCLOSE，算出日收益率，再取方差
def Variance120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Variance120"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=120).var().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Variance120"]]
    return stock_file


# 个股收益的20日偏度 Skewness20
# 取21个交易日的收盘价 TCLOSE 数据，计算日收益率，再计算其偏度
def Skewness20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Skewness20"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=20).skew().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Skewness20"]]
    return stock_file


# 个股收益的20日峰度 Kurtosis20
# 取21个交易日的收盘价数据，计算日收益率，再计算其峰度值
def Kurtosis20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Kurtosis20"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=20).kurt().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Kurtosis20"]]
    return stock_file


# 20日夏普比率 sharpe_ratio_20
# （Rp - Rf） / Sigma p 其中，Rp是个股的年化收益率，Rf是无风险利率（在这里设置为0.04），Sigma p是个股的收益波动率（标准差）
def sharpe_ratio_20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (
        (stock_file["close"] - stock_file["pre_close"]) * 365 / stock_file["pre_close"]
    )
    stock_file["RP"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=20).mean().values
    )
    stock_file["Sigma_p"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=20).std().values
    )
    stock_file["sharpe_ratio_20"] = (stock_file["RP"] - 0.04) / stock_file["Sigma_p"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "sharpe_ratio_20"]]
    return stock_file


# 个股收益的120日峰度 Kurtosis120
# 取121个交易日的收盘价数据，计算日收益率，再计算其峰度值
def Kurtosis120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Kurtosis120"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=120).kurt().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Kurtosis120"]]
    return stock_file


# 60日夏普比率 sharpe_ratio_60
# （Rp - Rf） / Sigma p 其中，Rp是个股的年化收益率，Rf是无风险利率（在这里设置为0.04），Sigma p是个股的收益波动率（标准差）
def sharpe_ratio_60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (
        (stock_file["close"] - stock_file["pre_close"]) * 360 / stock_file["pre_close"]
    )
    stock_file["RP"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=60).mean().values
    )
    stock_file["Sigma_p"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=60).std().values
    )
    stock_file["sharpe_ratio_60"] = (stock_file["RP"] - 0.04) / stock_file["Sigma_p"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "sharpe_ratio_60"]]
    return stock_file


# 个股收益的60日偏度 Skewness60
# 取61个交易日的收盘价数据，计算日收益率，再计算其偏度
def Skewness60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Skewness60"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=60).skew().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Skewness60"]]
    return stock_file


# 个股收益的120日偏度 Skewness120
# 取121个交易日的收盘价数据，计算日收益率，再计算其偏度
def Skewness120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Skewness120"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=121).skew().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Skewness120"]]
    return stock_file


# 120日夏普比率 sharpe_ratio_120
# （Rp - Rf） / Sigma p 其中，Rp是个股的年化收益率，Rf是无风险利率（在这里设置为0.04），Sigma p是个股的收益波动率（标准差）
def sharpe_ratio_120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (
        (stock_file["close"] - stock_file["pre_close"]) * 365 / stock_file["pre_close"]
    )
    stock_file["RP"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=120).mean().values
    )
    stock_file["Sigma_p"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=120).std().values
    )
    stock_file["sharpe_ratio_120"] = (stock_file["RP"] - 0.04) / stock_file["Sigma_p"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "sharpe_ratio_120"]]
    return stock_file


# 20日收益方差 Variance20
# 取21个交易日的收盘价，算出日收益率，再取方差
def Variance20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Variance20"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=20).var().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Variance20"]]
    return stock_file


# 60日收益方差 Variance60
# 取61个交易日的收盘价，算出日收益率，再取方差
def Variance60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Variance60"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=60).var().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Variance60"]]
    return stock_file


# 个股收益的60日峰度 Kurtosis60
# 取61个交易日的收盘价数据，计算日收益率，再计算其峰度值
def Kurtosis60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["Kurtosis60"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=61).kurt().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Kurtosis60"]]
    return stock_file


# # 技术指标因子


# 5日移动均线 MAC5
# 5日移动均线 / 今日收盘价close
def MAC5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_5"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=5).mean().values
    )
    stock_file["MAC5"] = stock_file["close_5"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MAC5"]]
    return stock_file


# 上轨线（布林线）指标 boll_up
# (MA(CLOSE,M)+2*STD(CLOSE,M)) / 今日收盘价; M=20
def boll_up(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ma_20"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=20).mean().values
    )
    stock_file["std_20"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=20).std().values
    )
    stock_file["boll_up"] = (
        stock_file["ma_20"] + 2 * stock_file["std_20"]
    ) / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "boll_up"]]
    return stock_file


# 26日指数移动均线 EMAC26
# 26日指数移动均线 / 今日收盘价close
def EMAC26(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_26"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=26).mean().values
    )
    stock_file["EMAC26"] = stock_file["close_26"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EMAC26"]]
    return stock_file


# 资金流量指标 MFI14
# ①求得典型价格（当日最高价high，最低价low和收盘价close的均值）②根据典型价格高低判定正负向资金流（资金流=典型价格*成交量volume）
# ③计算MR= 正向/负向 ④MFI=100-100/（1+MR）
def MFI14(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["典型价格"] = (
        stock_file["high"] + stock_file["low"] + stock_file["close"]
    ) / 3
    stock_file["典型价格_1"] = stock_file.groupby(["symbol"])["典型价格"].shift(1)
    stock_file["direction"] = stock_file["典型价格"] > stock_file["典型价格_1"]
    stock_file["资金流"] = stock_file["典型价格"] * stock_file["volume"]

    stock_file["positive"] = stock_file["direction"] * stock_file["资金流"]
    stock_file["negative"] = (1 - stock_file["direction"]) * stock_file["资金流"]

    stock_file["positive_all"] = (
        stock_file.groupby(["symbol"])["positive"].rolling(window=14).sum().values
    )
    stock_file["negative_all"] = (
        stock_file.groupby(["symbol"])["negative"].rolling(window=14).sum().values
    )

    stock_file["MFI14"] = 100 - (
        100 / (1 + (stock_file["positive_all"] / stock_file["negative_all"]))
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MFI14"]]
    return stock_file


# 120日指数移动均线 EMAC120
# 120日指数移动均线 / 今日收盘价close
def EMAC120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_120"] = (
        stock_file.groupby(["symbol"])["close"]
        .ewm(adjust=False, span=120)
        .mean()
        .values
    )
    stock_file["EMAC120"] = stock_file["close_120"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EMAC120"]]
    return stock_file


# 60日移动均线 MAC60
# 60日移动均线 / 今日收盘价close
def MAC60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_60"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=60).mean().values
    )
    stock_file["MAC60"] = stock_file["close_60"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MAC60"]]
    return stock_file


# 120日移动均线 MAC120
# 120日移动均线 / 今日收盘价close
def MAC120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_120"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=120).mean().values
    )
    stock_file["MAC120"] = stock_file["close_120"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MAC120"]]
    return stock_file


# 下轨线（布林线）指标 boll_down
# (MA(CLOSE,M)-2*STD(CLOSE,M)) / 今日收盘价; M=20 close
def boll_down(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_20mean"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=20).mean().values
    )
    stock_file["close_20std"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=20).std().values
    )
    stock_file["boll_down"] = (
        stock_file["close_20mean"] - 2 * stock_file["close_20std"]
    ) / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "boll_down"]]
    return stock_file


# 12日指数移动均线 EMAC12
# 12日指数移动均线 / 今日收盘价close
def EMAC12(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_12"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=12).mean().values
    )
    stock_file["EMAC12"] = stock_file["close_12"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EMAC12"]]
    return stock_file


# 5日指数移动均线 EMA5
# 5日指数移动均线 / 今日收盘价close
def EMA5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_5"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=5).mean().values
    )
    stock_file["EMA5"] = stock_file["close_5"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EMA5"]]
    return stock_file


# 20日指数移动均线 EMAC20
# 20日指数移动均线 / 今日收盘价close
def EMAC20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_20"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=20).mean().values
    )
    stock_file["EMAC20"] = stock_file["close_20"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EMAC20"]]
    return stock_file


# 20日移动均线 MAC20
# 20日移动均线 / 今日收盘价close
def MAC20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_20"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=20).mean().values
    )
    stock_file["MAC20"] = stock_file["close_20"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MAC20"]]
    return stock_file


# 平滑异同移动平均线 MACDC
# MACD(SHORT=12, LONG=26, MID=9) / 今日收盘价close
def MACDC(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA12"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=12).mean().values
    )
    stock_file["EMA26"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=26).mean().values
    )
    stock_file["VDIFF"] = stock_file["EMA12"] - stock_file["EMA26"]
    stock_file["VDIFF_EMA9"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=9).mean().values
    )
    stock_file["MACDC"] = stock_file["VDIFF"] - stock_file["VDIFF_EMA9"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MACDC"]]
    return stock_file


# 10日指数移动均线 EMAC10
# 10日指数移动均线 / 今日收盘价close
def EMAC10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA10"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=10).mean().values
    )
    stock_file["EMAC10"] = stock_file["EMA10"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "EMAC10"]]
    return stock_file


# 10日移动均线 MAC10
# 10日移动均线 / 今日收盘价close
def MAC10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_10"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=10).mean().values
    )
    stock_file["MAC10"] = stock_file["close_10"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MAC10"]]
    return stock_file


# 不复权价格因子 price_no_fq
# 不复权价格close
def price_no_fq(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["price_no_fq"] = stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "price_no_fq"]]
    return stock_file


# # 动量类因子 Market_data


# 5日乖离率 BIAS5
# （收盘价-收盘价的N日简单平均）/ 收盘价close的N日简单平均*100，在此n取5
def BIAS5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_5"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=5).mean().values
    )
    stock_file["BIAS5"] = (
        100 * (stock_file["close"] - stock_file["close_5"]) / stock_file["close_5"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "BIAS5"]]
    return stock_file


# 当前股价除以过去一年股价均值再减1 Price1Y
# 当日收盘价 / mean(过去一年(250天)的收盘价) -1
def Price1Y(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_250"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=250).mean().values
    )
    stock_file["Price1Y"] = (stock_file["close"] / stock_file["close_250"]) - 1
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Price1Y"]]
    return stock_file


# 当前股价除以过去三个月股价均值再减1 Price3M
# 当日收盘价 / mean(过去三个月(61天)的收盘价) -1
def Price3M(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_61"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=61).mean().values
    )
    stock_file["Price3M"] = (stock_file["close"] / stock_file["close_61"]) - 1
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Price3M"]]
    return stock_file


# 当前股价除以过去一个月股价均值再减1 Price1M
# 当日收盘价 / mean(过去一个月(21天)的收盘价) -1
def Price1M(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_21"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=21).mean().values
    )
    stock_file["Price1M"] = (stock_file["close"] / stock_file["close_21"]) - 1
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Price1M"]]
    return stock_file


# 12日收盘价格与日期线性回归系数 PLRC12
# 计算 12 日收盘价格，与日期序号（1-12）的线性回归系数，(close / mean(close)) = beta * t + alpha
def lin_regress_PLRC12(data):
    data = data.copy()
    if len(data) >= 12:
        data["x"] = list(range(1, 13))
        close_mean = data["close"].mean()
        data["close"] = data["close"] / close_mean
        lr = LinearRegression()
        lr.fit(data[["x"]], data[["close"]])
        return [lr.coef_[0][0]]
    else:
        return [np.nan]


def PLRC12(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file = stock_file.dropna(subset=["close"])
    a = pd.DataFrame(
        (lin_regress_PLRC12(x) for x in stock_file.groupby(["symbol"]).rolling(12))
    )
    stock_file["PLRC12"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "PLRC12"]]
    return stock_file


# BBI 动量 BBIC
# BBI(3, 6, 12, 24) / 收盘价 （BBI 为常用技术指标类因子“多空均线”）
def BBIC(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_3"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=3).mean().values
    )
    stock_file["close_6"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=4).mean().values
    )
    stock_file["close_12"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=12).mean().values
    )
    stock_file["close_24"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=24).mean().values
    )
    stock_file["BBIC"] = (
        stock_file["close_3"]
        + stock_file["close_6"]
        + stock_file["close_12"]
        + stock_file["close_24"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "BBIC"]]
    return stock_file


# 12日变动速率（Price Rate of Change） ROC12
# ①AX=今天的收盘价—12天前的收盘价 ②BX=12天前的收盘价 ③ROC=AX/BX*100
def ROC12(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["BX"] = stock_file.groupby(["symbol"])["close"].shift(12)
    stock_file["AX"] = stock_file["close"] - stock_file["BX"]
    stock_file["ROC12"] = stock_file["AX"] * 100 / stock_file["BX"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ROC12"]]
    return stock_file


# 120日变动速率（Price Rate of Change） ROC120
# ①AX=今天的收盘价—20天前的收盘价 ②BX=60天前的收盘价 ③ROC=AX/BX*100
def ROC120(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["BX"] = stock_file.groupby(["symbol"])["close"].shift(120)
    stock_file["AX"] = stock_file["close"] - stock_file["BX"]
    stock_file["ROC120"] = stock_file["AX"] * 100 / stock_file["BX"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ROC120"]]
    return stock_file


# 60日乖离率 BIAS60
# （收盘价-收盘价的N日简单平均）/ 收盘价的N日简单平均*100，在此n取60
def BIAS60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_60"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=60).mean().values
    )
    stock_file["BIAS60"] = (
        (stock_file["close"] - stock_file["close_60"]) * 100 / stock_file["close_60"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "BIAS60"]]
    return stock_file


# 20日变动速率（Price Rate of Change） ROC20
# ①AX=今天的收盘价—20天前的收盘价 ②BX=20天前的收盘价 ③ROC=AX/BX*100
def ROC20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["BX"] = stock_file.groupby(["symbol"])["close"].shift(20)
    stock_file["AX"] = stock_file["close"] - stock_file["BX"]
    stock_file["ROC20"] = stock_file["AX"] * 100 / stock_file["BX"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ROC20"]]
    return stock_file


# Aroon指标下轨 arron_down_25
# Aroon(下降)=[(计算期天数-最低价后的天数)/计算期天数]*100
def arron_down_25_1(data):
    data = data.copy()
    if len(data) >= 25:
        df1 = data["close"]
        df1 = list(df1)
        df1 = pd.DataFrame({"close": df1})
        df1["rank"] = df1["close"].rank(method="first")
        df1 = df1.reset_index(drop=True)
        num = 24 - list(df1[df1["rank"] == 1].index)[0]
        return [(25 - num) * 100 / 25]
    else:
        return [np.nan]


def arron_down_25(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    a = pd.DataFrame(
        (arron_down_25_1(x) for x in stock_file.groupby(["symbol"]).rolling(25))
    )
    stock_file["arron_down_25"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "arron_down_25"]]
    return stock_file


# 单日价量趋势12均值 single_day_VPT_12
# MA(single_day_VPT, 12)
def single_day_VPT_12(stock_file):
    stock_file = stock_file.copy()
    stock_file = single_day_VPT(stock_file)
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["single_day_VPT_12"] = (
        stock_file.groupby(["symbol"])["single_day_VPT"]
        .rolling(window=12)
        .mean()
        .values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "single_day_VPT_12"]]
    return stock_file


# 当前交易量相比过去1个月日均交易量 与过去过去20日日均收益率乘积 Volume1M
# 当日交易量 volume/ 过去20日交易量MEAN * 过去20日收益率MEAN
def Volume1M(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["volume_20_mean"] = (
        stock_file.groupby(["symbol"])["volume"].rolling(window=20).mean().values
    )
    stock_file["ret"] = (stock_file["close"] - stock_file["pre_close"]) / stock_file[
        "pre_close"
    ]
    stock_file["ret_20_mean"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=20).mean().values
    )
    stock_file["Volume1M"] = (
        stock_file["volume"] / stock_file["volume_20_mean"]
    ) * stock_file["ret_20_mean"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Volume1M"]]
    return stock_file


# 20日顺势指标 CCI20
# CCI:=(TYP-MA(TYP,N))/(0.015*AVEDEV(TYP,N)); TYP:=(HIGH+LOW+CLOSE)/3; N:=20
def CCI201(data):
    data = data.copy()
    if len(data) >= 20:
        list2 = []
        mean1 = data["TYP"].mean()
        data["a"] = abs(data["TYP"] - mean1)
        return [data["a"].mean()]
    else:
        return [np.nan]


def CCI20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TYP"] = (
        stock_file["high"] + stock_file["low"] + stock_file["close"]
    ) / 3
    stock_file["TYP_ma20"] = (
        stock_file.groupby(["symbol"])["TYP"].rolling(window=20).mean().values
    )
    a = pd.DataFrame((CCI201(x) for x in stock_file.groupby(["symbol"]).rolling(20)))
    stock_file["AVEDEV"] = list(a[0])
    stock_file["CCI20"] = (stock_file["TYP"] - stock_file["TYP_ma20"]) / (
        0.015 * stock_file["AVEDEV"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "CCI20"]]
    return stock_file


# 6日收盘价格与日期线性回归系数 PLRC6
# 计算 6 日收盘价格，与日期序号（1-6）的线性回归系数，(close / mean(close)) = beta * t + alpha
def lin_regress_PLRC6(data):
    data = data.copy()
    if len(data) >= 6:
        data["x"] = list(range(1, 7))
        close_mean = data["close"].mean()
        data["close"] = data["close"] / close_mean
        lr = LinearRegression()
        lr.fit(data[["x"]], data[["close"]])
        return [lr.coef_[0][0]]
    else:
        return [np.nan]


def PLRC6(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file = stock_file.dropna(subset=["close"])
    a = pd.DataFrame(
        (lin_regress_PLRC6(x) for x in stock_file.groupby(["symbol"]).rolling(6))
    )
    stock_file["PLRC6"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "PLRC6"]]
    return stock_file


# CR指标 CR20
# ①中间价=1日前的最高价+最低价/2
# ②上升值=今天的最高价-前一日的中间价（负值记0）
# ③下跌值=前一日的中间价-今天的最低价（负值记0）
# ④多方强度=20天的上升值的和，空方强度=20天的下跌值的和
# ⑤CR=（多方强度÷空方强度）×100
def CR20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["pre_high"] = stock_file.groupby(["symbol"])["high"].shift(1)
    stock_file["pre_low"] = stock_file.groupby(["symbol"])["low"].shift(1)
    stock_file["mid"] = (stock_file["pre_high"] + stock_file["pre_low"]) / 2
    stock_file["pre_mid"] = stock_file.groupby(["symbol"])["mid"].shift(1)

    stock_file["up"] = stock_file["high"] - stock_file["pre_mid"]
    stock_file["down"] = stock_file["pre_mid"] - stock_file["low"]

    stock_file.loc[stock_file[stock_file["up"] < 0].index, "up"] = 0
    stock_file.loc[stock_file[stock_file["down"] < 0].index, "down"] = 0

    stock_file["long"] = (
        stock_file.groupby(["symbol"])["up"].rolling(window=20).sum().values
    )
    stock_file["short"] = (
        stock_file.groupby(["symbol"])["down"].rolling(window=20).sum().values
    )

    stock_file["CR20"] = stock_file["long"] * 100 / stock_file["short"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "CR20"]]
    return stock_file


# 60日变动速率（Price Rate of Change） ROC60
# ①AX=今天的收盘价—20天前的收盘价 ②BX=60天前的收盘价 ③ROC=AX/BX*100
def ROC60(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["BX"] = stock_file.groupby(["symbol"])["close"].shift(60)
    stock_file["AX"] = stock_file["close"] - stock_file["BX"]
    stock_file["ROC60"] = stock_file["AX"] * 100 / stock_file["BX"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ROC60"]]
    return stock_file


# 20日乖离率 BIAS20
# （收盘价-收盘价的N日简单平均）/ 收盘价的N日简单平均*100，在此n取20
def BIAS20(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_20"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=20).mean().values
    )
    stock_file["BIAS20"] = (
        (stock_file["close"] - stock_file["close_20"]) * 100 / stock_file["close_20"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "BIAS20"]]
    return stock_file


# 多头力道 bull_power
# (最高价-EMA(close,13)) / close
def bull_power(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA13"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=13).mean().values
    )
    stock_file["bull_power"] = (stock_file["high"] - stock_file["EMA13"]) / stock_file[
        "close"
    ]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "bull_power"]]
    return stock_file


# Aroon指标上轨 arron_up_25
# Aroon(上升)=[(计算期天数-最高价后的天数)/计算期天数]*100
def arron_up_251(data):
    data = data.copy()
    if len(data) >= 25:
        data["rank"] = data["close"].rank(method="first")
        data = data.reset_index(drop=True)
        num = 24 - list(data[data["rank"] == 25].index)[0]
        return [(25 - num) * 100 / 25]
    else:
        return [np.nan]


def arron_up_25(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    a = pd.DataFrame(
        (arron_up_251(x) for x in stock_file.groupby(["symbol"]).rolling(25))
    )
    stock_file["arron_up_25"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "arron_up_25"]]
    return stock_file


# 单日价量趋势6日均值 single_day_VPT_6
# MA(single_day_VPT, 6)
def single_day_VPT_6(stock_file):
    stock_file = stock_file.copy()
    stock_file = single_day_VPT(stock_file)
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["single_day_VPT_6"] = (
        stock_file.groupby(["symbol"])["single_day_VPT"].rolling(window=6).mean().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "single_day_VPT_6"]]
    return stock_file


# 10日终极指标TRIX TRIX10
# MTR=收盘价的10日指数移动平均的10日指数移动平均的10日指数移动平均; TRIX=(MTR-1日前的MTR)/1日前的MTR*100
def TRIX10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA10"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=10).mean().values
    )
    stock_file["EMA1010"] = (
        stock_file.groupby(["symbol"])["EMA10"].ewm(adjust=False, span=10).mean().values
    )
    stock_file["MTR"] = (
        stock_file.groupby(["symbol"])["EMA1010"]
        .ewm(adjust=False, span=10)
        .mean()
        .values
    )
    stock_file["pre_MTR"] = stock_file.groupby(["symbol"])["MTR"].shift(1)

    stock_file["TRIX10"] = (
        (stock_file["MTR"] - stock_file["pre_MTR"]) * 100 / stock_file["pre_MTR"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "TRIX10"]]
    return stock_file


# 10日顺势指标 CCI10
# CCI:=(TYP-MA(TYP,N))/(0.015*AVEDEV(TYP,N)); TYP:=(HIGH+LOW+CLOSE)/3; N:=10
def CCI101(data):
    data = data.copy()
    if len(data) >= 10:
        list2 = []
        mean1 = data["TYP"].mean()
        data["a"] = abs(data["TYP"] - mean1)
        return [data["a"].mean()]
    else:
        return [np.nan]


def CCI10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TYP"] = (
        stock_file["high"] + stock_file["low"] + stock_file["close"]
    ) / 3
    stock_file["TYP_ma10"] = (
        stock_file.groupby(["symbol"])["TYP"].rolling(window=10).mean().values
    )
    a = pd.DataFrame((CCI101(x) for x in stock_file.groupby(["symbol"]).rolling(10)))
    stock_file["AVEDEV"] = list(a[0])
    stock_file["CCI10"] = (stock_file["TYP"] - stock_file["TYP_ma10"]) / (
        0.015 * stock_file["AVEDEV"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "CCI10"]]
    return stock_file


# 88日顺势指标 CCI88
# CCI:=(TYP-MA(TYP,N))/(0.015*AVEDEV(TYP,N)); TYP:=(HIGH+LOW+CLOSE)/3; N:=88
def CCI881(data):
    data = data.copy()
    if len(data) >= 88:
        list2 = []
        mean1 = data["TYP"].mean()
        data["a"] = abs(data["TYP"] - mean1)
        return [data["a"].mean()]
    else:
        return [np.nan]


def CCI88(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TYP"] = (
        stock_file["high"] + stock_file["low"] + stock_file["close"]
    ) / 3
    stock_file["TYP_ma88"] = (
        stock_file.groupby(["symbol"])["TYP"].rolling(window=88).mean().values
    )
    a = pd.DataFrame((CCI881(x) for x in stock_file.groupby(["symbol"]).rolling(88)))
    stock_file["AVEDEV"] = list(a[0])
    stock_file["CCI88"] = (stock_file["TYP"] - stock_file["TYP_ma88"]) / (
        0.015 * stock_file["AVEDEV"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "CCI88"]]
    return stock_file


# 24日收盘价格与日期线性回归系数 PLRC24
# 计算 24 日收盘价格，与日期序号（1-24）的线性回归系数， (close / mean(close)) = beta * t + alpha
def lin_regress_PLRC24(data):
    data = data.copy()
    if len(data) >= 24:
        data["x"] = list(range(1, 25))
        close_mean = data["close"].mean()
        data["close"] = data["close"] / close_mean
        lr = LinearRegression()
        lr.fit(data[["x"]], data[["close"]])
        return [lr.coef_[0][0]]
    else:
        return [np.nan]


def PLRC24(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file = stock_file.dropna(subset=["close"])
    a = pd.DataFrame(
        (lin_regress_PLRC24(x) for x in stock_file.groupby(["symbol"]).rolling(24))
    )
    stock_file["PLRC24"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "PLRC24"]]
    return stock_file


# 10日乖离率 BIAS10
# （收盘价-收盘价的N日简单平均）/ 收盘价的N日简单平均*100，在此n取10
def BIAS10(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_10"] = (
        stock_file.groupby(["symbol"])["close"].rolling(window=10).mean().values
    )
    stock_file["BIAS10"] = (
        (stock_file["close"] - stock_file["close_10"]) * 100 / stock_file["close_10"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "BIAS10"]]
    return stock_file


# 6日变动速率（Price Rate of Change） ROC6
# ①AX=今天的收盘价—6天前的收盘价 ②BX=6天前的收盘价 ③ROC=AX/BX*100
def ROC6(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["BX"] = stock_file.groupby(["symbol"])["close"].shift(6)
    stock_file["AX"] = stock_file["close"] - stock_file["BX"]
    stock_file["ROC6"] = stock_file["AX"] * 100 / stock_file["BX"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "ROC6"]]
    return stock_file


# 当前价格处于过去1年股价的位置 fifty_two_week_close_rank
# 取过去的250个交易日各股的收盘价时间序列，每只股票按照从大到小排列，并找出当日所在的位置
def fifty_two_week_close_rank(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["fifty_two_week_close_rank"] = (
        stock_file.groupby(["symbol"])["close"]
        .rolling(window=250)
        .rank(method="min", ascending=False)
        .values
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "fifty_two_week_close_rank"]
    ]
    return stock_file


# 梅斯线 MASS
# MASS(N1=9, N2=25, M=6)
# =（最高价-最低价的N1日简单移动平均）/（最高价-最低价的N1日简单移动平均的N1日简单移动平均的N2日累和）
# MAMASS=MASS的M日简单移动平均
def MASS(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["range"] = stock_file["high"] - stock_file["low"]
    stock_file["range_9"] = (
        stock_file.groupby(["symbol"])["range"].rolling(window=9).mean().values
    )
    stock_file["range_9_9"] = (
        stock_file.groupby(["symbol"])["range_9"].rolling(window=9).mean().values
    )
    stock_file["a"] = stock_file["range_9"] / stock_file["range_9_9"]

    stock_file["MASS"] = (
        stock_file.groupby(["symbol"])["a"].rolling(window=25).sum().values
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "MASS"]]
    return stock_file


# 空头力道 bear_power
# (最低价-EMA(close,13)) / close
def bear_power(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA13"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=13).mean().values
    )
    stock_file["bear_power"] = (stock_file["low"] - stock_file["EMA13"]) / stock_file[
        "close"
    ]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "bear_power"]]
    return stock_file


# 单日价量趋势 single_day_VPT
# （今日收盘价 - 昨日收盘价）/ 昨日收盘价 * 当日成交量 # (复权方法为基于当日前复权)!!!
def single_day_VPT(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["single_day_VPT"] = (
        (stock_file["close"] - stock_file["pre_close"])
        * stock_file["volume"]
        / stock_file["pre_close"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "single_day_VPT"]]
    return stock_file


# 5日终极指标TRIX TRIX5
# MTR=收盘价的5日指数移动平均的10日指数移动平均的5日指数移动平均; TRIX=(MTR-1日前的MTR)/1日前的MTR*100
def TRIX5(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["EMA5"] = (
        stock_file.groupby(["symbol"])["close"].ewm(adjust=False, span=5).mean().values
    )
    stock_file["EMA55"] = (
        stock_file.groupby(["symbol"])["EMA5"].ewm(adjust=False, span=5).mean().values
    )
    stock_file["MTR"] = (
        stock_file.groupby(["symbol"])["EMA55"].ewm(adjust=False, span=5).mean().values
    )
    stock_file["pre_MTR"] = stock_file.groupby(["symbol"])["MTR"].shift(1)

    stock_file["TRIX5"] = (
        (stock_file["MTR"] - stock_file["pre_MTR"]) * 100 / stock_file["pre_MTR"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "TRIX5"]]
    return stock_file


# 1减去 过去一个月收益率排名与股票总数的比值 Rank1M
# 1-(Rank(个股20日收益) / 股票总数)
def Rank1M(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["close_21"] = stock_file.groupby(["rpt_date"])["close"].shift(21)
    stock_file["ret"] = (stock_file["close"] - stock_file["close_21"]) / stock_file[
        "close_21"
    ]
    stock_file["Rank"] = (
        stock_file.groupby(["rpt_date"])["ret"]
        .rank(method="min", ascending=True)
        .values
    )
    stock_file["a"] = 1
    stock_file1 = stock_file.groupby(["rpt_date"])["a"].sum().reset_index()
    stock_file.drop(["a"], axis=1, inplace=True)
    stock_file = pd.merge(stock_file, stock_file1, on=["rpt_date"], how="left")
    stock_file["Rank1M"] = 1 - (stock_file["Rank"] / stock_file["a"])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "Rank1M"]]
    return stock_file


# 15日顺势指标 CCI15
# CCI:=(TYP-MA(TYP,N))/(0.015*AVEDEV(TYP,N)); TYP:=(HIGH+LOW+CLOSE)/3; N:=15
def CCI151(data):
    data = data.copy()
    if len(data) >= 15:
        list2 = []
        mean1 = data["TYP"].mean()
        data["a"] = abs(data["TYP"] - mean1)
        return [data["a"].mean()]
    else:
        return [np.nan]


def CCI15(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["TYP"] = (
        stock_file["high"] + stock_file["low"] + stock_file["close"]
    ) / 3
    stock_file["TYP_ma15"] = (
        stock_file.groupby(["symbol"])["TYP"].rolling(window=15).mean().values
    )
    a = pd.DataFrame((CCI151(x) for x in stock_file.groupby(["symbol"]).rolling(15)))
    stock_file["AVEDEV"] = list(a[0])
    stock_file["CCI15"] = (stock_file["TYP"] - stock_file["TYP_ma15"]) / (
        0.015 * stock_file["AVEDEV"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "CCI15"]]
    return stock_file


# # 风险因子-风格因子


# 市值因子 size
# 资产规模 = 1.0 * 对数总资产 ttl_ast = 总资产的对数
def size(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["size"] = np.log(stock_file["ttl_ast"])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "size"]]
    return stock_file


# 杠杆因子 leverage
# 0.38 * 市场杠杆 + 0.35 * 资产负债率 + 0.27 * 账面杠杆
def leverage(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file1 = market_leverage(stock_file)
    stock_file2 = debt_to_assets(stock_file)
    stock_file3 = book_leverage(stock_file)
    stock_file12 = pd.merge(
        stock_file1, stock_file2, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file123 = pd.merge(
        stock_file12, stock_file3, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file123["leverage"] = (
        stock_file123["market_leverage"] * 0.38
        + stock_file123["debt_to_assets"] * 0.35
        + stock_file123["book_leverage"] * 0.27
    )
    stock_file123 = stock_file123[["symbol", "rpt_date", "pub_date", "leverage"]]
    return stock_file123


# 动量因子 momentum
# 相对强弱 relative_strength
# 动量因子=1.0*相对强弱因子=sum(w_t * ln(1 + r_t))，其中r_t取滞后21个交易日的前504个交易日的close数据，
# w_t为半衰期为126天的指数权重，满足w(t-126)=0.5*w(t)
def momentum1(data, W):
    data = data.copy()
    if len(data) >= 504:
        data["ret_21"] = np.log(data["ret_21"] + 1)
        data["weigh"] = W
        data["m"] = data["weigh"] * data["ret_21"]
        return [data["m"].sum()]
    else:
        return [np.nan]


def momentum(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (
        stock_file["close"] / stock_file["pre_close"]
        - stock_file["index_close"] / stock_file["pre_index_close"]
    )
    stock_file["ret_21"] = stock_file.groupby(["symbol"])["ret"].shift(21)

    half_life = 126
    window = 504
    L, Lambda = 0.5 ** (1 / half_life), 0.5 ** (1 / half_life)
    W = []
    for i in range(window):
        W.append(Lambda)
        Lambda *= L
    W = np.array(W[::-1])
    a = pd.DataFrame(
        (momentum1(x, W) for x in stock_file.groupby(["symbol"]).rolling(504))
    )
    stock_file["momentum"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "momentum"]]
    return stock_file


# 非线性市值因子 non_linear_size
##市值立方因子 cube_of_size
# 1.0*市值立方因子，标准化市值因子 ttl_ast 的三次方，之后将结果和标准化市值因子回归取残差（去除和市值因子的共线性），
# 然后残差值进行缩尾处理（将3倍标准差之外的点处理成3倍标准差）和标准化


def winsorize(data, factor_name, scale=1, inclusive=True, inf2nan=True):
    value = data.copy()
    value = value.pivot(index="rpt_date", columns="symbol", values=factor_name)
    long = value.shape[0]
    for i in range(long):
        s = value.iloc[i, :]
        if inf2nan == True:
            s[np.isinf(s)] = np.nan
            mu = np.mean(s.dropna())
            std = np.std(s.dropna(), ddof=1)
            up = mu + scale * std
            down = mu - scale * std
            if inclusive == True:
                s[s > up] = up
                s[s < down] = down
            else:
                s[s > up] = np.nan
                s[s < down] = np.nan
        else:
            mu = np.mean(s.dropna())
            std = np.std(s.dropna(), ddof=1)
            up = mu + scale * std
            down = mu - scale * std
            if inclusive == True:
                s[s > up] = up
                s[s < down] = down
            else:
                s[s > up] = np.nan
                s[s < down] = np.nan
    value = (
        value.reset_index()
        .melt(id_vars="rpt_date")
        .rename(columns={"value": factor_name})
    )
    return value


def non_linear_size(stock_file):
    stock_file = stock_file[["symbol", "pub_date", "rpt_date", "ttl_ast"]].copy()

    # 转化为日度数据
    factor_name = "ttl_ast"
    stock_unique = np.unique(list(stock_file["symbol"]))
    stock_file = stock_file.sort_values(by=["symbol", "pub_date", "rpt_date"])
    stock_file.drop_duplicates(subset=["symbol", "pub_date"], keep="last", inplace=True)

    stock_file = stock_file.pivot(
        index="pub_date", columns="symbol", values=factor_name
    )
    # 日度数据
    stock_file = stock_file.reset_index()
    a = pd.DataFrame({"pub_date": [str(date.today())]})
    stock_file = pd.concat([stock_file, a], axis=0)
    stock_file["rpt_date"] = pd.to_datetime(stock_file["pub_date"]).dt.to_period("D")
    stock_file = (
        stock_file.set_index("rpt_date").resample("D").asfreq().reset_index().ffill()
    )
    stock_file["rpt_date"] = stock_file["rpt_date"].dt.to_timestamp()
    stock_file.drop(["pub_date"], inplace=True, axis=1)
    stock_file = stock_file.melt(id_vars="rpt_date").rename(
        columns={"value": factor_name, "variable": "symbol"}
    )
    stock_file = stock_file.dropna().reset_index(drop=True)

    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["size"] = np.log(stock_file["ttl_ast"])
    stock_file = stock_file[["symbol", "rpt_date", "size"]]

    date_values = stock_file["rpt_date"].unique()
    df = pd.DataFrame()
    for j in list(date_values):
        df1 = stock_file[stock_file["rpt_date"] == j].copy()
        df1["size"][np.isinf(df1["size"])] = np.nan
        df1["size_3"] = df1["size"] ** 3
        std3 = df1["size_3"].std()
        mean3 = df1["size_3"].mean()
        df1["standard_size_3"] = (df1["size_3"] - mean3) / std3

        std1 = df1["size"].std()
        mean1 = df1["size"].mean()
        df1["standard_size_1"] = (df1["size"] - mean1) / std1

        df1 = df1.dropna(subset=["standard_size_1", "standard_size_3"])
        if df1.empty:
            continue

        lr = LinearRegression()
        lr.fit(df1[["standard_size_1"]], df1[["standard_size_3"]])
        a = lr.predict(df1[["standard_size_1"]])
        a = np.array(a)
        b = a.flatten()
        b = b.tolist()
        df1["predict"] = b
        df1["predict"] = df1["standard_size_3"] - df1["predict"]

        df1 = winsorize(df1, "predict", scale=3)
        df1["non_linear_size"] = (df1["predict"] - df1["predict"].mean()) / df1[
            "predict"
        ].std()
        df = pd.concat([df, df1], axis=0)
    df = df.reset_index()
    df["pub_date"] = df["rpt_date"]
    stock_file = df[["symbol", "rpt_date", "pub_date", "non_linear_size"]]
    return stock_file


# 市净率因子 book_to_price_ratio
# 每股净资产 ttl_eqy 与每股股价 close 的比率
def book_to_price_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["book_to_price_ratio"] = stock_file["ttl_eqy"] / stock_file["close"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "book_to_price_ratio"]]
    return stock_file


# 流动性因子 liquidity
# 0.35 * 月换手率 + 0.35 * 季度平均平均月换手率 + 0.3 * 年度平均月换手率，
# 之后将结果和市值因子做回归，取残差（去除和市值因子的共线性）
def liquidity(stock_file):
    stock_file = stock_file.copy()
    stock_file1 = share_turnover_monthly(stock_file)
    stock_file2 = average_share_turnover_quarterly(stock_file)
    stock_file3 = average_share_turnover_annual(stock_file)
    stock_file4 = size(stock_file)
    stock_file = pd.merge(
        stock_file1, stock_file2, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file = pd.merge(
        stock_file, stock_file3, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file = pd.merge(
        stock_file, stock_file4, on=["symbol", "pub_date", "rpt_date"]
    )

    stock_file["liquidity1"] = (
        0.35 * stock_file["share_turnover_monthly"]
        + 0.35 * stock_file["average_share_turnover_quarterly"]
        + 0.3 * stock_file["average_share_turnover_annual"]
    )
    stock_file = stock_file.dropna(subset=["liquidity1", "size"])
    symbol_values = stock_file["symbol"].unique()
    df = pd.DataFrame()
    for j in list(symbol_values):
        df1 = stock_file[stock_file["symbol"] == j].copy()
        lr = LinearRegression()
        lr.fit(df1[["size"]], df1[["liquidity1"]])
        a = lr.predict(df1[["size"]])
        a = np.array(a)
        b = a.flatten()
        b = b.tolist()
        df1["liquidity"] = df1["liquidity1"] - b
        df = pd.concat([df, df1], axis=0)
    df = df.reset_index()
    stock_file = df[["symbol", "rpt_date", "pub_date", "liquidity"]]
    return stock_file


# 现金流量市值比 cash_earnings_to_price_ratio
# 过去一年的净经营现金流 net_cf_oper 除以 当前股票市值 TOTMKTCAP
def cash_earnings_to_price_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["cash_earnings"] = (
        stock_file.groupby(["symbol"])["net_cf_oper"].rolling(window=252).sum().values
    )
    stock_file["cash_earnings_to_price_ratio"] = (
        stock_file["cash_earnings"] / stock_file["TOTMKTCAP"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "cash_earnings_to_price_ratio"]
    ]
    return stock_file


# 残差历史波动率 historical_sigma
# 残差历史波动率 historical_sigma：计算beta时的回归残差项的过去252个交易日的标准差。
def lin_regress_historical_sigma(data):
    data = data.copy()
    if len(data) >= 252:
        lr = LinearRegression()
        lr.fit(data[["ret"]], data[["index_ret"]])
        a = lr.predict(data[["ret"]])
        a = np.array(a)
        b = a.flatten()
        b = b.tolist()
        data["predict"] = b
        data["predict"] = data["index_ret"] - data["predict"]
        return [data["predict"].std()]
    else:
        return [np.nan]


def historical_sigma(stock_file):
    stock_file = stock_file[
        [
            "symbol",
            "rpt_date",
            "pub_date",
            "close",
            "pre_close",
            "index_close",
            "pre_index_close",
        ]
    ].copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = (stock_file["close"] / stock_file["pre_close"] - 1) * 251 - 0.04
    stock_file["index_ret"] = (
        stock_file["index_close"] / stock_file["pre_index_close"] - 1
    ) * 251 - 0.04
    stock_file = stock_file.dropna()
    a = pd.DataFrame(
        (
            lin_regress_historical_sigma(x)
            for x in stock_file.groupby(["symbol"]).rolling(252)
        )
    )
    stock_file["historical_sigma"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "historical_sigma"]]
    return stock_file


# 5年盈利增长率 earnings_growth
# 过去5个财年 年均EPS增长 除以 年均EPS 基本每股收益BASICEPS
def earnings_growth(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["BASICEPS_y"] = (
        stock_file.groupby(["symbol"])["eps_base"].rolling(window=4).mean().values
    )
    stock_file["BASICEPS_y_5"] = stock_file.groupby(["symbol"])["BASICEPS_y"].shift(20)
    stock_file["a"] = stock_file["BASICEPS_y"] - stock_file["BASICEPS_y_5"]
    stock_file["earnings_growth"] = stock_file["a"] / stock_file["BASICEPS_y"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "earnings_growth"]]
    return stock_file


# 利润市值比 earnings_to_price_ratio
# 过去一年的净利润 net_prof 除以 当前股票市值TOTMKTCAP，等于 PE 的倒数
def earnings_to_price_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["net_prof_y"] = (
        stock_file.groupby(["symbol"])["net_prof"].rolling(window=252).sum().values
    )
    stock_file["earnings_to_price_ratio"] = (
        stock_file["net_prof_y"] / stock_file["TOTMKTCAP"]
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "earnings_to_price_ratio"]
    ]
    return stock_file


# 市场杠杆 market_leverage
# (普通股市值 + 优先股账面价值(中国股票为0) + 长期负债账面价值 LTMDEBT) / 普通股市值TOTMKTCAP，长期负债账面价值=长期借款+应付债券
# (普通股市值 + 长期负债账面价值) / 普通股市值，长期负债账面价值=长期借款 lt_ln+应付债券 bnd_pay
def market_leverage(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["market_leverage"] = (
        stock_file["TOTMKTCAP"] + stock_file["lt_ln"] + stock_file["bnd_pay"]
    ) / stock_file["TOTMKTCAP"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "market_leverage"]]
    return stock_file


# 对数总市值 natural_log_of_market_cap
# 对数总市值=总市值的对数
def natural_log_of_market_cap(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["natural_log_of_market_cap"] = np.log(stock_file["TOTMKTCAP"])
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "natural_log_of_market_cap"]
    ]
    return stock_file


# 季度平均平均月换手率 average_share_turnover_quarterly
# ln(sum(turn_over_ratio)/3)，turn_over_ratio为过去三个月（63个交易日）的平均换手率
def average_share_turnover_quarterly(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["Turnover_Rate_mean"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=63).mean().values
    )
    stock_file["share_turnover_q"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate_mean"]
        .rolling(window=63)
        .sum()
        .values
    )
    stock_file["average_share_turnover_quarterly"] = np.log(
        stock_file["share_turnover_q"] / 3
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "average_share_turnover_quarterly"]
    ]
    return stock_file


# 收益离差 cumulative_range
# 收益离差 cumulative_range：过去12个月中月收益率（以21个交易日为一个月）的最大值和最小值之间的差异。
# 股票需上市需超过6个月，否则结果为nan。
def cumulative_range(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["pre_close"] = stock_file.groupby(["symbol"])["close"].shift(21)
    stock_file["ret_m"] = (stock_file["close"] / stock_file["pre_close"]) - 1
    stock_file["ret_m_1"] = stock_file.groupby(["symbol"])["ret_m"].shift(21)
    stock_file["ret_m_2"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 2)
    stock_file["ret_m_3"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 3)
    stock_file["ret_m_4"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 4)
    stock_file["ret_m_5"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 5)
    stock_file["ret_m_6"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 6)
    stock_file["ret_m_7"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 7)
    stock_file["ret_m_8"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 8)
    stock_file["ret_m_9"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 9)
    stock_file["ret_m_10"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 10)
    stock_file["ret_m_11"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 11)
    stock_file["ret_m_12"] = stock_file.groupby(["symbol"])["ret_m"].shift(21 * 12)
    stock_file["cumulative_range_max"] = stock_file[
        [
            "ret_m_1",
            "ret_m_2",
            "ret_m_3",
            "ret_m_4",
            "ret_m_5",
            "ret_m_6",
            "ret_m_7",
            "ret_m_8",
            "ret_m_9",
            "ret_m_10",
            "ret_m_11",
            "ret_m_12",
        ]
    ].max(axis=1)
    stock_file["cumulative_range_min"] = stock_file[
        [
            "ret_m_1",
            "ret_m_2",
            "ret_m_3",
            "ret_m_4",
            "ret_m_5",
            "ret_m_6",
            "ret_m_7",
            "ret_m_8",
            "ret_m_9",
            "ret_m_10",
            "ret_m_11",
            "ret_m_12",
        ]
    ].min(axis=1)
    stock_file["cumulative_range"] = (
        stock_file["cumulative_range_max"] - stock_file["cumulative_range_min"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "cumulative_range"]]
    return stock_file


# 月换手率 share_turnover_monthly
# ln(sum(turn_over_ratio))，turn_over_ratio为过去21个交易日的换手率
def share_turnover_monthly(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["share_turnover_m"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"].rolling(window=21).sum().values
    )
    stock_file["share_turnover_monthly"] = np.log(stock_file["share_turnover_m"])
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "share_turnover_monthly"]
    ]
    return stock_file


# 账面杠杆 book_leverage
# (普通股账面价值 ttl_eqy+ 长期负债账面价值 LTMDEBT) / 普通股账面价值
def book_leverage(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["book_leverage"] = (
        stock_file["ttl_eqy"] + stock_file["LTMDEBT"]
    ) / stock_file["ttl_eqy"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "book_leverage"]]
    return stock_file


# 成长因子 growth
# 0.18 * 预期长期盈利增长率 + 0.11 * 预期短期盈利增长率 + 0.24 * 5年盈利增长率 + 0.47 * 5年营业收入增长率


# 盈利预期因子 earnings_yield
# 0.68 * 预期市盈率 + 0.21 * 营业收益市值比 + 0.11 * 利润市值比
def earnings_yield(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["earnings_yield"] = (
        0.68 * stock_file["conPe"]
        + 0.21 * stock_file["PSTTM"]
        + 0.11 * stock_file["PELFY"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "earnings_yield"]]
    return stock_file


# 年度平均月换手率 average_share_turnover_annual
# ln(sum(turn_over_ratio)/12)，turn_over_ratio为过去十二个月（252个交易日）的平均换手率
def average_share_turnover_annual(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["Turnover_Rate"] = stock_file["volume"] * 100 / stock_file["share_circ"]
    stock_file["Turnover_Rate_y"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate"]
        .rolling(window=252)
        .mean()
        .values
    )
    stock_file["share_turnover_y"] = (
        stock_file.groupby(["symbol"])["Turnover_Rate_y"]
        .rolling(window=252)
        .sum()
        .values
    )
    stock_file["average_share_turnover_annual"] = np.log(
        stock_file["share_turnover_y"] / 12
    )
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "average_share_turnover_annual"]
    ]
    return stock_file


# 日收益率标准差 daily_standard_deviation#
# sqrt(sum(w_t*(r_t - r_mean)**2))，其中r_t为过去252个交易日的日收益率，w_t为半衰期为42个交易日的指数权重
# 满足w(t-42)=0.5*w(t)
def daily_standard_deviation1(data, W):
    data = data.copy()
    if len(data) >= 252:
        r_mean = data["ret"].mean()
        data["ret"] = data["ret"] - r_mean
        data["ret"] = data["ret"] ** 2
        data["W"] = W
        data["A"] = data["ret"] * data["W"]
        return [math.sqrt(data["A"].sum())]
    else:
        return [np.nan]


def daily_standard_deviation(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = np.log(stock_file["close"] / stock_file["pre_close"])
    stock_file["ret_mean"] = (
        stock_file.groupby(["symbol"])["ret"].rolling(window=252).mean().values
    )

    half_life = 42
    window = 252
    L, Lambda = 0.5 ** (1 / half_life), 0.5 ** (1 / half_life)
    W = []
    for i in range(window):
        W.append(Lambda)
        Lambda *= L
    W = np.array(W[::-1])

    a = pd.DataFrame(
        (
            daily_standard_deviation1(x, W)
            for x in stock_file.groupby(["symbol"]).rolling(252)
        )
    )
    stock_file["daily_standard_deviation"] = list(a[0])
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "daily_standard_deviation"]
    ]
    return stock_file


# 资产负债率 debt_to_assets
# 总负债账面价值 ttl_liab / 总资产账面价值 ttl_ast
def debt_to_assets(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["debt_to_assets"] = stock_file["ttl_liab"] / stock_file["ttl_ast"]
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "debt_to_assets"]]
    return stock_file


# 5年营业收入增长率 sales_growth
# 过去5个财年的 每股营业收入增长 inc_oper(营业收入) 除以 年均每股营业收入
def sales_growth(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["OPREVPS"] = stock_file["inc_oper"] / stock_file["share_total"]
    stock_file["OPREVPS_y"] = (
        stock_file.groupby(["symbol"])["OPREVPS"].rolling(window=4).sum().values
    )
    stock_file["OPREVPS_y_5"] = stock_file.groupby(["symbol"])["OPREVPS"].shift(20)
    stock_file["OPREVPS_y_mean"] = (
        stock_file.groupby(["symbol"])["OPREVPS"].rolling(window=20).mean().values
    )
    stock_file["OPREVPS_y_mean"] = stock_file["OPREVPS_y_mean"] * 4
    stock_file["sales_growth"] = (
        (stock_file["OPREVPS_y"] - stock_file["OPREVPS_y_5"])
        * 5
        / stock_file["OPREVPS_y_mean"]
    )
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "sales_growth"]]
    return stock_file


# 残差波动因子 residual_volatility
# 0.74 * 日收益率标准差(DASTD) + 0.16 * 收益离差(CMRA) + 0.1 * 残差历史波动率(HSIGMA)，
# 之后将结果和beta因子，市值因子做回归，取残差
def residual_volatility(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file1 = daily_standard_deviation(stock_file)
    stock_file2 = cumulative_range(stock_file)
    stock_file3 = historical_sigma(stock_file)
    stock_file12 = pd.merge(
        stock_file1, stock_file2, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file123 = pd.merge(
        stock_file12, stock_file3, on=["symbol", "pub_date", "rpt_date"]
    )

    stock_file123["residual_volatility"] = (
        stock_file123["daily_standard_deviation"] * 0.74
        + stock_file123["cumulative_range"] * 0.16
        + stock_file123["historical_sigma"] * 0.1
    )
    stock_file4 = size(stock_file)
    stock_file5 = beta(stock_file)
    stock_file1234 = pd.merge(
        stock_file123, stock_file4, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file12345 = pd.merge(
        stock_file1234, stock_file5, on=["symbol", "pub_date", "rpt_date"]
    )
    stock_file = stock_file12345.copy()

    residual_volatility = []
    stock_file = stock_file.dropna(subset=["size", "beta", "residual_volatility"])
    symbol_values = stock_file["symbol"].unique()
    df = pd.DataFrame()
    for j in list(symbol_values):
        df1 = stock_file[stock_file["symbol"] == j].copy()
        df1 = df1.replace([np.inf, -np.inf], np.nan)
        df1.dropna(inplace=True)
        lr = LinearRegression()
        lr.fit(df1[["size", "beta"]], df1[["residual_volatility"]])
        a = lr.predict(df1[["size", "beta"]])
        b = a.flatten()
        b = b.tolist()
        df1["residual_volatility"] = df1["residual_volatility"] - b
        df = pd.concat([df, df1], axis=0)
    stock_file = df[["symbol", "rpt_date", "pub_date", "residual_volatility"]]
    return stock_file


# BETA beta
# 一元线性回归求解 beta=sum(w_t*(r_t-r_mean)*(R_t-R_mean))/sum(w_t*(R_t-R_mean)^2)，
# 其中r_t、R_t分别使用前252个交易日的股票和中证流通指数的close数据，
# w_t为半衰期为63个交易日的指数加权平均权重，w_t=0.5**(t/63)
def beta1(data, W):
    data = data.copy()
    if len(data) >= 252:
        r_mean = data["ret"].mean()
        R_mean = data["index_ret"].mean()

        data["ret"] = data["ret"] - r_mean
        data["index_ret"] = data["index_ret"] - R_mean
        data["W"] = W
        data["a"] = data["W"] * data["ret"] * data["index_ret"]
        data["B"] = data["W"] * (data["index_ret"] ** 2)
        beta = data["a"].sum() / data["B"].sum()
        return [beta]
    else:
        return [np.nan]


def beta(stock_file):
    stock_file = stock_file.copy()
    stock_file = stock_file.sort_values(["symbol", "rpt_date"]).reset_index(drop=True)
    stock_file["ret"] = stock_file["close"] / stock_file["pre_close"] - 1
    stock_file["index_ret"] = (
        stock_file["index_close"] / stock_file["pre_index_close"] - 1
    )

    half_life = 63
    window = 252
    L, Lambda = 0.5 ** (1 / half_life), 0.5 ** (1 / half_life)
    W = []
    for i in range(window):
        W.append(Lambda)
        Lambda *= L
    W = np.array(W[::-1])

    a = pd.DataFrame((beta1(x, W) for x in stock_file.groupby(["symbol"]).rolling(252)))
    stock_file["beta"] = list(a[0])
    stock_file = stock_file[["symbol", "rpt_date", "pub_date", "beta"]]
    return stock_file


# 预期市盈率 predicted_earnings_to_price_ratio
# 分析师对未来一年预期盈利加权平均值 除以 当前股票市值 conPe
def predicted_earnings_to_price_ratio(stock_file):
    stock_file = stock_file.copy()
    stock_file["predicted_earnings_to_price_ratio"] = 1 / stock_file["conPe"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "predicted_earnings_to_price_ratio"]
    ]
    return stock_file


# 预期短期盈利增长率 short_term_predicted_earnings_growth
# 分析师预测未来1年盈利增长率 conProfitYoy
def short_term_predicted_earnings_growth(stock_file):
    stock_file = stock_file.copy()
    stock_file["short_term_predicted_earnings_growth"] = stock_file["conProfitYoy"]
    stock_file = stock_file[
        ["symbol", "rpt_date", "pub_date", "short_term_predicted_earnings_growth"]
    ]
    return stock_file


# 预期长期盈利增长率 long_term_predicted_earnings_growth
# 分析师预测未来3-5年盈利增长率
