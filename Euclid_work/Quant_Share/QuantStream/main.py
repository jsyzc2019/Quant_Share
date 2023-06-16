"""
# -*- coding: utf-8 -*-
"""

from Euclid_work.Quant_Share import info_lag, reindex, data2score
import streamlit_echarts
import streamlit as st
import pandas as pd
from funcs import data_prepare, read_file, bkTest, IC_Calc
from plots import nav_plot, ICIR_plot

# 基本网页设置
st.set_page_config(page_title='因子回测', layout='wide')
st.markdown("<h1 style='text-align: center; color: black;'>因子回测分析结果</h1>", unsafe_allow_html=True)
st.markdown("<h1 style='text-align: center; color: black;'>Factor Backtesting Analysis</h1>", unsafe_allow_html=True)
st.sidebar.header('回测参数设置')
st.sidebar.write('请完成以下基本回测参数设置')
st.sidebar.text(
    '基本事项说明：\n1. 当前支持的回测范围：2015年1月1日至2023年5月31日\n2. 研究标的对照：\n中证500 000905.XSHG\n中证1000 000852.XSHG\n沪深300 000300.XSHG\n3. 回测默认分5组')

# 用户交互参数设置
# 起始时间
start_date = st.sidebar.date_input('回测起始日期', value=pd.to_datetime('20180101'))
# 截至时间
end_date = st.sidebar.date_input('回测截至日期', value=pd.to_datetime('20221231'))
# 绘图起始
plot_begin = st.sidebar.date_input('绘图开始日期', value=pd.to_datetime('20200531'))
# 绘图截至
plot_end = st.sidebar.date_input('绘图结束日期', value=pd.to_datetime('20221231'))
# 研究标的
bench_code = st.sidebar.selectbox("研究标的", ('000852.XSHG', '000905.XSHG', '000300.XSHG'))

tradeDateCol_factor = st.text_input('因子日期列名称', value='tradeDate')
uploaded_factor = st.file_uploader("## uploader wide factor data", type=['h5', 'csv'])
tradeDateCol_rtn = st.text_input('回报率日期列名称', value='tradeDate')
uploaded_rtn = st.file_uploader("## uploader wide return data", type=['h5', 'csv'])

factor = None
score = None
rtn = None

factor = read_file(uploaded_factor, tradeDateCol_factor)
if factor is not None:
    factor.index = pd.to_datetime(factor.index)
    factor = reindex(factor)
    score = info_lag(data2score(factor), n_lag=1)
    st.write("因子数据前五行为：")
    st.write(factor.head())
rtn = read_file(uploaded_rtn, tradeDateCol_rtn)


if st.sidebar.button('准备基础数据'):
    DataClass = data_prepare(start_date, end_date, bench_code)

if st.sidebar.button("分组回测指标"):
    outMetrics, group_res = bkTest(score, start_date, end_date, bench_code)
    st.write("### 回测结束, 各组指标如下")
    st.dataframe(outMetrics.set_index('group'))

if st.sidebar.button("分组净值绘图"):
    outMetrics, group_res = bkTest(score, start_date, end_date, bench_code)
    streamlit_echarts.st_pyecharts(nav_plot(group_res, plot_begin, plot_end, bench_code), height="500px", width="100%")

if st.sidebar.button("因子ICIR绘图"):
    rankIC, IR = IC_Calc(rtn, score, start_date, end_date)
    streamlit_echarts.st_pyecharts(ICIR_plot(rankIC, IR, plot_begin, plot_end), height="500px", width="100%")
