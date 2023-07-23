meta_gm_data_update_to_PG 用于GM数据至postgres数据库

各文件单独运行更新, 也可以运行update_to_PG.py实现批量更新

`get_fundamentals`有限制, 每25次请求后都被要求等待
[利润表数据](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#stk-get-fundamentals-income-%E6%9F%A5%E8%AF%A2%E5%88%A9%E6%B6%A6%E8%A1%A8%E6%95%B0%E6%8D%AE)
