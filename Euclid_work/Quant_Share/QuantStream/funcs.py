import streamlit as st
import pandas as pd
from Euclid_work.Quant_Share.BackTest_Meng import DataPrepare, simpleBT
from Euclid_work.Quant_Share import get_tradeDate, get_data, reindex, info_lag, data2score
import numpy as np
from scipy.stats import spearmanr
import h5py
import tempfile

def get_nav_data_2_plot(data: pd.Series):
    return data / data.iloc[0] - 1

@st.cache_resource
def data_prepare(_start_date, _end_date, _bench_code):
    # group beck data prepare
    _DataClass = DataPrepare(beginDate=_start_date, endDate=_end_date, bench=_bench_code[3:6])
    _DataClass.get_Tushare_data()
    return _DataClass

def read_file(uploaded_file, tradeDateCol):
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        st.write(f"正在读取：{uploaded_file.name}")
        if uploaded_file.name.endswith('csv'):
            data = pd.read_csv(uploaded_file)
            data = data.set_index(tradeDateCol)
            return data
        elif uploaded_file.name.endswith('h5'):
            # 创建临时文件
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            # 将上传的文件内容保存到临时文件
            temp_file.write(uploaded_file.read())
            # 关闭临时文件
            temp_file.close()
            file_path = temp_file.name
            data = pd.read_hdf(file_path)
            data = data.set_index(tradeDateCol)
            # st.write(data)
            return data
        else:
            raise NotImplemented(uploaded_file.name.split('.')[-1])

def factor2score(factor):
    factor.index = pd.to_datetime(factor.index)
    factor = reindex(factor)
    score = info_lag(data2score(factor), n_lag=1)
    st.write("因子数据前五行为：")
    st.write(factor.head())
    return factor, score

@st.cache_resource
def bkTest(_score, _start_date, _end_date, _bench_code):
    # group beck test
    _DataClass = data_prepare(_start_date, _end_date, _bench_code)
    BTClass = simpleBT(_DataClass.TICKER, _DataClass.BENCH)
    if _score is not None:
        tmpScore = _score
    else:
        tmpScore = _DataClass.Score
    _, _outMetrics, _group_res = BTClass.groupBT(tmpScore)
    return _outMetrics, _group_res

@st.cache_resource
def IC_Calc(_rtn, _score, _start_date, _end_date):
    if _rtn is None:
        price_df = get_data('MktEqud', begin=_start_date, end=_end_date)
        _rtn = reindex(price_df.pivot(index='tradeDate', columns='ticker', values='chgPct'))
        st.write('自动获取回报率数值')

    _score.dropna(how='all', axis=0, inplace=True)
    rankIC = pd.DataFrame(np.zeros((_score.shape[0], 1)), index=_score.index, columns=['rankIC'])
    for index, values in _score.iterrows():
        if index in _rtn.index:
            tmp = pd.concat([values, _rtn.loc[index]], axis=1, ignore_index=True)
            tmp.dropna(axis=0, inplace=True)
            tmp = tmp.rank(axis=0)
            # rank
            res = spearmanr(tmp)
            if res.pvalue > 0.05:
                rankIC.loc[index] = 0
            else:
                rankIC.loc[index] = res.correlation
    rankIC.dropna(axis=0, inplace=True)
    IR = (rankIC.mean()) / rankIC.std()
    return rankIC, IR