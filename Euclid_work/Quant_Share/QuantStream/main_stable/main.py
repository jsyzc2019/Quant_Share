"""
# -*- coding: utf-8 -*-
"""

from Euclid_work.Quant_Share import BARRA, DATA_READY
import streamlit_echarts
import streamlit as st
import pandas as pd
from funcs import data_prepare, read_file, bkTest, IC_Calc, factor2score
from plots import nav_plot, ICIR_plot

# 基本网页设置
st.set_page_config(page_title="Quant Share Factor Kit", layout="wide")
st.title(":blue[Quant Share] Factor Kit")
st.divider()

global factor, score, rtn, DataClass
factor = score = rtn = None


# 用户交互参数设置
with st.sidebar:
    st.header("回测参数设置")
    with st.expander("基本事项说明"):
        st.text(
            "\n(1)当前支持的回测范为`2015年1月1日`至`2023年5月31日`\n(2)研究标的对照：\n中证500 000905.XSHG\n中证1000 000852.XSHG\n沪深300 000300.XSHG\n(3)回测默认分5组"
        )

    calc_col, plot_col = st.columns(2)
    with calc_col:
        # 起始时间
        start_date = st.date_input("回测起始日期", value=pd.to_datetime("20200531"))
        # 绘图起始
        plot_begin = st.date_input("绘图开始日期", value=pd.to_datetime("20200531"))
    with plot_col:
        # 截至时间
        end_date = st.date_input("回测截至日期", value=pd.to_datetime("20221231"))
        # 绘图截至
        plot_end = st.date_input("绘图结束日期", value=pd.to_datetime("20221231"))

    # 研究标的
    bench_code = st.selectbox("研究标的", ("000852.XSHG", "000905.XSHG", "000300.XSHG"))

    st.divider()

    if st.button("准备基础数据", key="sidebar"):
        with st.spinner("Wait for it..."):
            DataClass = data_prepare(start_date, end_date, bench_code)
        st.success("基础数据准备完成!", icon="✅")


factor_backtest, factor_analysis, factor_barra = st.tabs(
    ["📈 因子回测", "🗃 因子分析", "🧐 BARRA"]
)

with factor_backtest:
    with st.expander("基本事项说明"):
        st.text("计算分组净值绘图流程为依次点击：准备基础数据（侧边栏） -> 分组回测指标计算 -> 分组净值绘图")

    text_input, file_upload = st.columns(2)
    factor_backtest_date_col = text_input.text_input(
        "因子日期列名称", value="tradeDate", key="backtest_factor_date"
    )
    factor_backtest_uploaded = file_upload.file_uploader(
        "## 上传因子数据（宽表格式）", type=["h5", "csv"], key="backtest_factor_uploaded"
    )

    detect = text_input.checkbox("自动检测日期列", key="backtest_date_detect")
    if detect and factor_backtest_uploaded is not None:
        factor, factor_backtest_date_col = read_file(
            factor_backtest_uploaded, auto=True
        )
        st.info(f"自动检测的日期列名为：{factor_backtest_date_col}")
    elif factor_backtest_uploaded is not None:
        factor, factor_backtest_date_col = read_file(
            factor_backtest_uploaded, factor_backtest_date_col
        )
    if factor is not None:
        factor, score = factor2score(factor)
    st.divider()

    col1, col2 = st.columns(2, gap="small")

    if col1.button("分组回测指标计算"):
        with st.spinner("Wait for it..."):
            outMetrics, group_res = bkTest(
                score, start_date, end_date, bench_code, key="factor"
            )
            st.write("### 回测结束, 各组指标如下")
            st.dataframe(outMetrics.set_index("group"))
        st.success("分组回测指标计算完成!", icon="✅")
    if col2.button("分组净值绘图"):
        with st.spinner("Wait for it..."):
            outMetrics, group_res = bkTest(
                score, start_date, end_date, bench_code, key="factor"
            )
            streamlit_echarts.st_pyecharts(
                nav_plot(group_res, plot_begin, plot_end, bench_code),
                height="500px",
                width="100%",
            )
        st.success("分组净值绘图完成!", icon="✅")

with factor_analysis:
    date_name_col, file_upload_col = st.columns(2)
    with date_name_col:
        factor_analysis_factor_date_col = st.text_input(
            "因子日期列名称", value="tradeDate", key="factor_analysis_factor_date"
        )
        factor_analysis_factor = st.file_uploader(
            "## 上传因子数据（宽表格式）", type=["h5", "csv"], key="factor_analysis_factor_uploaded"
        )

    with file_upload_col:
        factor_analysis_rtn_date_col = st.text_input(
            "回报率日期列名称", value="tradeDate", key="factor_analysis_rtn_date"
        )
        factor_analysis_rtn = st.file_uploader(
            "## 上传回报率数据（宽表格式）", type=["h5", "csv"], key="factor_analysis_rtn_uploaded"
        )

    detect = st.checkbox("自动检测日期列", key="factor_analysis_date_detect")

    if detect and factor_analysis_factor is not None:
        factor, factor_analysis_factor_date_col = read_file(
            factor_analysis_factor, auto=True
        )
        if factor is not None:
            factor, score = factor2score(factor)
            st.info(f"因子数据自动检测的日期列名为：{factor_analysis_factor_date_col}")
        if factor_analysis_rtn is not None:
            rtn, factor_analysis_rtn_date_col = read_file(
                factor_analysis_rtn, auto=True
            )
            st.info(f"回报率数据自动检测的日期列名为：{factor_analysis_rtn_date_col}")
    elif factor_analysis_factor is not None:
        factor, factor_analysis_factor_date_col = read_file(
            factor_analysis_factor, factor_analysis_factor_date_col
        )
        if factor is not None:
            factor, score = factor2score(factor)
        if factor_analysis_rtn is not None:
            rtn, factor_analysis_rtn_date_col = read_file(
                factor_analysis_rtn, factor_analysis_rtn_date_col
            )

    st.divider()

    if st.button("因子ICIR绘图"):
        if score is not None:
            with st.spinner("Wait for it..."):
                rankIC, IR = IC_Calc(rtn, score, start_date, end_date)
                streamlit_echarts.st_pyecharts(
                    ICIR_plot(rankIC, IR, plot_begin, plot_end),
                    height="500px",
                    width="100%",
                )
            st.success("因子ICIR绘图完成!")
        else:
            st.warning("未检测到因子数据!")


def get_factor(barra: BARRA, factor_name):
    return getattr(barra, factor_name)


with factor_barra:
    with st.expander("目前已经整合的BARRA因子"):
        st.text(",".join(DATA_READY["factor"]))
    barra = BARRA(beginDate=start_date, endDate=end_date)
    factor_name = st.selectbox("BARRA因子", DATA_READY["factor"])
    if st.button("获取选定因子"):
        factor = get_factor(barra, factor_name)
        factor, score = factor2score(factor, verbose=False)
        st.write(f"{factor_name}因子的前五行为：")
        st.write(factor.head())
    if st.button("BARRA因子回测"):
        with st.spinner("Wait for it..."):
            factor = get_factor(barra, factor_name)
            factor, score = factor2score(factor, verbose=False)
            outMetrics, group_res = bkTest(
                score, start_date, end_date, bench_code, key=factor_name
            )
            st.write("### 回测结束, 各组指标如下")
            st.dataframe(outMetrics.set_index("group"))
            st.success("分组回测指标计算完成!", icon="✅")
            streamlit_echarts.st_pyecharts(
                nav_plot(group_res, plot_begin, plot_end, bench_code),
                height="500px",
                width="100%",
            )
            st.success("分组净值绘图完成!", icon="✅")
