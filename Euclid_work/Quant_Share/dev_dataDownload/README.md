此文件夹下的文件，用于从掘金`gm`，通联`uqer`，东方财富`EM`获取数据并存储至本地

## *dev_data_load*

> 用于通联本地SDK的数据存储，正逐步迁移至`dev_uqerUpdate`

其中单个函数即为单个表的数据获取函数，并以`rolling_save`方式获取，指定`begin`, `end`,  `freq`后，每次获取一个`span`的数据后，随即写入。然后再进行下一个`span`。

## *dev_gmDownload*

> 用于`gm`的数据的首次更新，指定时间区间进行存储

由`meta_gm_dataDownLoad`中调用想要的表更新函数，命名方式为`表名_upate`。通过遍历股票`symbol`获得全量数据，然后进行存储，默认存储至`dataBase_root_path_gmStockFactor`。

## *dev_gmUpdate*

> 用于`gm`数据的增量更新，直接根据本地文件的最后修改时间进行增量更新

使用`get_table_info`获取本地文件信息，然后进行增量更新，可同时指定多表批量进行更新。

## *dev_uqerUpdate*

>用于`uqer`数据的增量更新，使用的可以是本地`SDK`，也可以是`webAPI`，目前为webAPI方式
>
>本地`SDK`更方便，切换方式为设置meta_uqer_dataDownLoad/base_package中的DataAPI的导入源，如果是FakeDataAPI则为webAPI, 如果为uqer则为本地SDK

目前支持增量更新的表有四张，后续根据实际需求更新

- `MktEqud`: *沪深股票日行情*
- `MktLimit`: 沪深涨跌停限制
- `FdmtIndiRtnPit`: *财务指标**—**盈利能力* *(Point in time)*
- `FdmtIndiPSPit`:  *财务指标**—**每股* *(Point in time)*

## *dev_EM*
东财数据的下载及更新函数，相关信息应当存放在`Euclid_work/Quant_Share/dev_dataDownload/meta_EM_dataDownLoad/codes_info`中。

对于新信息，请存放于`new.json`文件中。测试通过后，将其归类进index,future,stock等文件中。

- **Download**：根据新增信息进行批量下载，以及部分功能测试。
- **Update**：对已入库的数据进行批量更新，较为成熟的模块才可在这里使用。

## Attention

gm数据仅在服务器能进行更新 ，因为需要打开并登录掘金客户端

`Uqer`本地`SDK`仅在本地能更新，因为token保留在本地，但`WebAPI`都可
> 20230722 通联[WebAPI](https://mall.datayes.com/mydata/purchasedData)试用期已过, 无法使用

`EM`的数据仅能在服务器上更新，因为仅在服务器上配置好了登陆文件。
