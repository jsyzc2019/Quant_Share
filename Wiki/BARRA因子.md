

# BARRA Factor

Quant Share中内置了部分因子的计算，位于模块`FactorCalc`中。目前暂不支持传入外界数据，现数据进来自于Quant Share本地的数据。后续会考虑接入用户自定义的数据。

## 因子模板`FactorBase`

后续所有因子类基于该类创建。需传入beginDate和endDate，日期格式为`%Y%m%d`

```python
def __init__(self, beginDate: str = None, endDate: str = None, **kwargs) -> None:
```

### `regress`

回归函数，可进行带权重的回归，默认忽略无效值。

```python
def regress(y, X, intercept: bool = True, weight: int = 1, verbose: bool = True):
    '''
    :param y: 因变量
    :param X: 自变量
    :param intercept: 是否有截距
    :param weight: 权重
    :param verbose: 是否返回残差
    :return:
    '''
```

### `align_data`

基于index和columns进行表格的对齐。传入形式可为`align_data([df1, df2], df3, df4)`

```python
 def align_data(self, data_lst: list[pd.DataFrame] = [], *args):
    '''
    基于index和columns进行表格的对齐
    :param data_lst: 以列表形式传入需要对齐的表格
    :param args: 可以传入单个表格
    :return: 
    '''
```

### `winsorize`

对离群值进行截断。有`mad`和`tile`两种方式。

```python
def winsorize(
    series: pd.Series or np.ndarray,
    n: float or int = 3,
    mode: str = 'mad',
    **kwargs
    ):
    '''
    :param series: 
    :param n: 考虑离群值的范围，默认为3，对mad方法生效
    :param mode: 控制截断方式，有mad和tile两种方法
    :param kwargs: 可传入limits （eg. [0.1,0.1]）,对tile方法生效
    :return: 
    '''
```

### `nomalize_zscore`

中值化，采用Z-score的形式。

$$
    x'=\frac{X-\mu}{\sigma}
$$

### `BackTest`

对宽表形式的因子进行简单回测，回测采用Quant Share内置的DataClass作为基准，采用simpleBT进行回测。

```python
def BackTest(self, data: pd.DataFrame, DataClass=None):
```

## `SizeFactor`

规模因子。

## `Liquidity`

流动性因子。