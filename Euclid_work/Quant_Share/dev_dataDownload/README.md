此文件夹下的文件，用于从掘金gm，通联`DataYes`获取数据并存储至本地

## *dev_data_load*

滚动储存，可设置频率为季度q，年度y，月度m

多用于`DataYes`

## *dev_gmDownload*

遍历股票`symbol`获得全量数据，然后分年度存储

相较于*dev_data_load*，更适用于gm，速度较快

## Attention

gm数据仅在服务器能进行更新 ，因为需要打开并登录掘金客户端

`DataYes`仅在本地能更新，因为token保留在本地