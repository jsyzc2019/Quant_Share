from Euclid_work.Quant_Share import get_tradeDate, get_data
from pyecharts.charts import Line
from pyecharts.charts import Bar
from pyecharts.charts import Scatter
import pyecharts.options as opts
from funcs import get_nav_data_2_plot
import streamlit as st

def nav_plot(_group_res, _plot_begin, _plot_end):
    global bench_code
    _plot_begin = get_tradeDate(_plot_begin, 0)
    _plot_end = get_tradeDate(_plot_end, -1)

    bench_nav = _group_res['4'][0] / _group_res['4'][2]

    # 回测累计收益率图
    x_data = bench_nav.loc[_plot_begin:_plot_end].index.strftime("%Y%m%d").tolist()
    line = (
        Line(
            init_opts=opts.InitOpts(
                width='1000px',
                height='600px',
                theme="white"
            )
        )
        .add_xaxis(xaxis_data=x_data)
        .add_yaxis(
            series_name="group 1",
            y_axis=get_nav_data_2_plot(_group_res['{}'.format(1)][0].loc[_plot_begin:_plot_end]).tolist(),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="group 2",
            y_axis=get_nav_data_2_plot(_group_res['{}'.format(2)][0].loc[_plot_begin:_plot_end]).tolist(),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="group 3",
            y_axis=get_nav_data_2_plot(_group_res['{}'.format(3)][0].loc[_plot_begin:_plot_end]).tolist(),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="group 4",
            y_axis=get_nav_data_2_plot(_group_res['{}'.format(4)][0].loc[_plot_begin:_plot_end]).tolist(),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="group 5",
            y_axis=get_nav_data_2_plot(_group_res['{}'.format(5)][0].loc[_plot_begin:_plot_end]).tolist(),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="{}".format(bench_code),
            y_axis=get_nav_data_2_plot(bench_nav.loc[_plot_begin:_plot_end]).tolist(),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="回测累计收益率"),  # 标题参数
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
        )
        .set_series_opts(
            linestyle_opts=opts.LineStyleOpts(
                is_show=True,
                width=2,
            )
        )
    )
    return line

def ICIR_plot(rankIC, IR, _plot_begin, _plot_end):
    _plot_begin = get_tradeDate(_plot_begin, 0)
    _plot_end = get_tradeDate(_plot_end, -1)
    rankIC_lim = rankIC.loc[_plot_begin:_plot_end]
    st.write(f"IC数据量为{len(rankIC_lim)}")
    bar = (
        Bar(
            init_opts=opts.InitOpts(
                width='1000px',
                height='600px',
                theme="white"
            )
        )
        .add_xaxis(xaxis_data=rankIC_lim.index.strftime("%Y%m%d").tolist())
        .add_yaxis(
            series_name="IC",
            y_axis=rankIC_lim['rankIC'].tolist(),
            itemstyle_opts=opts.ItemStyleOpts(color="#00CD96"))
        .set_global_opts(
            title_opts={"text": f"IR={float(IR)}"},
            brush_opts=opts.BrushOpts(),  # 设置操作图表的画笔功能
            toolbox_opts=opts.ToolboxOpts(),  # 设置操作图表的工具箱功能
            yaxis_opts=opts.AxisOpts(name="IC"),
            # 设置Y轴名称、定制化刻度单位
            xaxis_opts=opts.AxisOpts(name="日期"),  # 设置X轴名称

        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    return bar