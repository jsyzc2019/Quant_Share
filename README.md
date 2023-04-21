此项目拟使用Python构建一套便捷可用的投研框架，道阻且长，持续更新，分版块说明在[Wiki](https://gitee.com/Euclid-Jie/Quant_Share/wikis/%E5%BF%AB%E9%80%9F%E4%BD%BF%E7%94%A8)中

# 快速使用

## 配置数据文件路径

本项目需依赖一些本地数据文件，下载到本地后，配置文件路径即可使用，其中`dataFile` 和 `Fut_Data`均数据储存路径

于`Eucld_get_data.py`中配置`dataBase_root_path`和`dataBase_root_path_future`即可，具体为

```python
dataBase_root_path = r'D:\Share\Euclid_work\dataFile'  # 股票数据
dataBase_root_path_future = r"D:\Share\Fut_Data"  # 期货数据
```

## 使用get_data调用数据

由工具包导入函数后，即可调用，以下为示例

### 获取股票数据

```python
from Utils import get_data
data = get_data('bench_price', begin='20160101', end=None, ticker='905')
print(data.info())
```

### 获取期货数据

```python
from Utils import get_data
data = get_data('Price_Volume_Data/main', begin='20160101', end=None, ticker='A')
print(data.info())
```

### 已支持导入的表

查看最新的[更新](https://gitee.com/Euclid-Jie/Quant_Share/wikis/%E5%B7%B2%E6%94%AF%E6%8C%81%E7%9A%84%E8%A1%A8)

目前的表多位`ts`组织形式，即时间轴`t`和个体标识`s`双重索引，其中`date_cokumn`与`get_data`中的`begin`，`end`区间一致，`ticker_column`与`get_data`中的`ticker`一致

| tabelName                 | description        | assets | date_column | ticker_column |
| ------------------------- | ------------------ | ------ | ----------- | ------------- |
| bench_price               | 指数数据           | stock  | trade_date  | symbol        |
| stock_price               | 股票价格           | stock  | trade_date  | symbol        |
| HKshszHold                | 外资持股           | stock  | endDate     | ticker        |
| MktEqud                   | 沪深股票日行情     | stock  | tradeDate   | ticker        |
| MktLimit                  | 沪深股票日涨跌限制 | stock  | tradeDate   | ticker        |
| ResConSecCorederi         | 一致预期信息表     | stock  | repForeDate | secCode       |
| Broker_Data               | 期货数据           | future | date        |               |
| Price_Volume_Data/main    | 期货价格数据       | future | bob         |               |
| Price_Volume_Data/submain | 期货价格数据       | future | bob         |               |

需要说明的是，期货数据的组织形式与股票不同，每个期货品种的数据单独存储，故`ticker`直接指定期货品种代码即可，不设`ticker_column`

## 使用回测

下面展示一个外置持股5日变动因子的计算、回测过程

### 调用数据计算因子

需要注意的是`Score`需为宽表格式(`index`为日期，`column`为`stockNum`)，并需传入`reindex`对宽表进行标准化

`info_lag`是将日期滞后一天，即在后一个交易日才能获取此因子数据，严禁使用未来信息

```python
from Utils import *

# score prepare
data = get_data('HKshszHold', begin='20200101', end='20221231')
data = data.pivot(index='endDate', columns='ticker', values='partyPct')
data = reindex(data.pct_change(periods=5, axis=0))
score = info_lag(data2score(data), n_lag=1)  # do not use future info
```

### 准备回测基础数据

准备股票的日收益率，`ST`状况、限价信息等

```python
# group beck data prepare
DataClass = DataPrepare(beginDate='20200101', endDate='20221231')
DataClass.get_Tushare_data()
```

### 使用因子`Score`进行分组回测

```python
# group beck test
BTClass = simpleBT(DataClass.TICKER, DataClass.BENCH)
fig, outMetrics, group_res = BTClass.groupBT(score)
fig.show()  # 绘图
print(outMetrics)  # 输出指标
```

### 回测结果展示

五组的超额净值`alpha_Nav`曲线图

![分组回测超额结果](https://euclid-picgo.oss-cn-shenzhen.aliyuncs.com/image/202304141126903.png)

### 回测指标说明

| Metric   | description  | Calc Method                                              |
| -------- | ------------ | -------------------------------------------------------- |
| group    | 组别         | `Score`越大，组别数越小                                  |
| totalrtn | 总超额收益   | 回测区间内的总超额收益，`nav[-1] - nav[0]`得到           |
| alzdrtn  | 年化超额收益 | 根据回测区间进行调整，计算方式`(totalrtn+1)^(1/years)-1` |
| stdev    | 总标准差     | 超额曲线的标准差                                         |
| vol      | 年化波动     | 根据`stdev*std(250)`得到                                 |
| sharpe   | 夏普比例     | 根据`alzdrtn/vol`得到                                    |
| maxdown  | 最大回测     | 专业术语，不多解释                                       |
| turnover | 换手率       | 回测区间内的平均每日换手率                               |

## 函数、常量说明

除`get_data`外，本项目内置大量常量和函数，方便投研过程中调用，现进行说明

### 函数

#### `get_data`

根据表名获取数据

```python
get_data(tableName, begin='20150101', end=None, sources='gm', fields: list = None, ticker: list = None)
```

#### `get_table_info`

根据表名，获取数据表的信息

```python
get_table_info(tableName)
```

#### `format_date`

将`str`或`int`格式的日期转为`datetime`格式

```python
from Utils import *
format_date('20210101')  # return Timestamp('2021-01-01 00:00:00')
```

#### `format_stockCode`

兼容各种股票代码格式，标准化为`wind`格式，即`000001.SZ`形式

```python
from Utils import *
format_stockCode('000001')  # return '000001.sz'
format_stockCode('000001.XSHE')  # return '000001.sz'
```

#### `format_futures`

兼容各种期货代码，简化为简称，即`IC`格式

```python
from Utils import *
format_futures('CFFEX.IC')  # return 'IC'
```

#### `readPkl`

读取`PKL`文件

#### `savePkl`

储存`PKL`文件

#### `data2score`

将宽表格式的数据，根据`rank`转化为分数

```python
data2score(data, neg=False, ascending=True, axis=1)
"""
use rank as score
:param data:
:param neg: if Ture, score span [-1, 1], default FALSE
:param ascending:
:param axis:
:return:
"""
```

#### `reindex`

将宽表数据转化为统一格式，即`index`为`datetime`，`column`为`wind`格式的股票代码

需要注意的是，无论传入的`column`有多少，统一使用`stockList`进行`reindex`，即变为5219列

而`index`是根据，传入数据的范围，进行填补。如果有缺失，则补全为`np.NAN`

#### `info_lag`

将宽表数据进行滞后，与传统`shift`不同，直接对`index`进行操作，不会损失数据

```python
info_lag(data, n_lag)
"""
Delay the time corresponding to the data by n trading days
:param data:
:param n_lag:
:return:
"""
```

#### `save_data_h5`

将数据存储为`.h5`文件，仅支持传入`DataFrame`

```python
save_data_h5(toSaveData, name, subPath='dataFile', reWrite=False)
"""
Store the pd.Data Frame data as a file in .h5 format
:param toSaveData:
:param name: file name will to store
:param subPath: The path to store the file will be 'cwd/subPath/name.h5', default 'dataFile'
:param reWrite: if Ture, will rewrite file, default False
:return:
"""
```

#### `get_tradeDate`

获取交易日信息，根据传入不同的n，获取不同的交易日信息

```python
get_tradeDate(date, n=0)
"""
Returns the date related to date based on the setting of n
if n = 0, will return the future the nearest trade date, if date is trade date, will return itself
if n = -1, will return the backward the nearest trade date, if date is trade date, will return itself
else will returns information from the delay n days (calendar, tradeDate_fore and tradeDate_back）
:param date:
:param n: default 0
:return:
"""
```

### 常量

#### `tableInfo`

储存数据表信息，格式为`dict`

```python
from Utils import tableInfo
tableInfo.keys()  # 查看目前能调用数据表

tableInfo['bench_price']  # 查看bench_price的具体信息
```

#### `stock_info`

沪深股票基本信息，`DataFrame`格式

```python
from Utils import stock_info
stock_info.head()
```

#### `stockList`

`wind`格式股票代码，由`stock_info`标准化后得到，共5219个

```python
from Utils import stockList
stockList
```

#### `stockNumList`

纯数字格式股票代码，由`stock_info`标准化后得到，共5219个

#### `bench_info`

指数基本信息，`DataFrame`格式

```python
from Utils import bench_info
bench_info.head()
```

#### `tradeDate_info`

交易日信息，`DataFrame`格式

```python
from Utils import tradeDate_info
tradeDate_info.head()
```

#### `tradeDateList`

交易日列表，覆盖2015-2023，格式为标准化后的`Timestamp`，也即`datetime`

```python
from Utils import tradeDateList
tradeDateList
```

#### `futures_list`

收录的期货简称，目前有71个，可以自行扩充

```python
from Utils import futures_list
futures_list
```

#### `quarter_begin`

每季度开始的月份日期

```python
from Utils import quarter_begin
quarter_begin
```

#### `quarter_end`

每季度结束的月份日期

```python
from Utils import quarter_end
quarter_end
```

# 项目说明

## 目录说明

```shell
- Euclid_work
  - dataFile  # 股票数据文件
  	- bench_price  # 具体表名
  	- ...
  - Utils
  	- init.py  # 库配置文件
  	- BackTest.py  # 回测工具
  	- Eucld_get_data.py  # get_data函数
  	- Utils.py  # 工具函数及常量
- Fut_Data  # 期货数据文件
	- Broker_Data  # 具体表名
- README.md  
```

## 关于数据储存形式

`dataFile` 和 `Fut_Data`均数据储存路径，但两者存储组织形式不同，更推荐第一种

### `dataFile`

`dataFile`为数据文件的`root`路径，其下每一个文件夹代表一个数据表，分年份、季度存储为`.h5`文件

关于`.h5`文件，需要说的是，如果写入的是一个空的`DataFrame`也是不会报错的，所以并非所有的文件均存储有效数据，显然`HKshszHold_Y2023_Q4.h5`无数据

```shell
- dataFile
	- bench_price
		- bench_price_Y2015.h5
		- bench_price_Y2016.h5
		...
		- bench_price_Y2023.h5
	- HKshszHold
		- HKshszHold_Y2015_Q1.h5
		- HKshszHold_Y2015_Q2.h5
		...
		- HKshszHold_Y2023_Q4.h5
```

### `Fut_Data`

`Fut_Data`为兼容数据路径，多为期货数据。因为此数据在本框架之前存储，故进行兼容，后续数据组织形式将遵从`dataFile`方式

此组织形式中，没种期货品种为一个文件，且有两个来源`gm`:掘金，`qe`:未知，`qe`数据缺失，故默认导出`gm`数据源。两个来源的文件命名有差别，但已兼容。

此外，如果表中有树形机构，例如`Price_Volume_Data`下设`main`和`submain`可以通过设置表名来实现兼容，例`Price_Volume_Data/main`

```shell
- Fut_Data
	- Broker_Data
		- 2016
			- gm
				- CFFEX.IC.h5
				- CFFEX.IC.h5
			- qe
				- A_qe.h5
				- AG_qe.h5
		...
		- 2023
	- Price_Volume_Data
		- main
			- 2016
				- gm
					- CFFEX.IC_gm.h5
					- CFFEX.IF_gm.h5
				- qe
					- A_qe.h5
			...
			- 2023
		- submain
			- 2016
			...
			- 2023
```

