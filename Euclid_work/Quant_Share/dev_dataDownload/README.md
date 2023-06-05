此文件夹下的文件，用于从掘金gm，通联`DataYes`获取数据并存储至本地

## *dev_data_load*

滚动储存，可设置频率为季度q，年度y，月度m

多用于`DataYes`

## *dev_gmDownload*

遍历股票`symbol`获得全量数据，然后分年度存储

相较于*dev_data_load*，更适用于gm，速度较快

## *dev_EM*
东财数据的下载及更新函数，相关信息应当存放在`Euclid_work/Quant_Share/dev_dataDownload/meta_EM_dataDownLoad/codes_info`中。

对于新信息，请存放于`new.json`文件中。测试通过后，将其归类进index,future,stock等文件中。

- **Download**：根据新增信息进行批量下载，以及部分功能测试。
- **Update**：对已入库的数据进行批量更新，较为成熟的模块才可在这里使用。

## Attention

gm数据仅在服务器能进行更新 ，因为需要打开并登录掘金客户端

`DataYes`仅在本地能更新，因为token保留在本地

`EM`的数据仅能在服务器上更新，因为仅在服务器上配置好了登陆文件。
