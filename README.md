# OmniRuntim统一加速引擎
OmniRuntime提供开源的大数据实时、离线统一的向量化加速方案。
## OmniRuntime架构图
![image.png](https://raw.gitcode.com/user-images/assets/7320433/0c7ce56f-0a6f-4a55-af3a-b72353dc897f/image.png 'image.png')



## 社区
鲲鹏社区首页
- https://kunpeng-beta.rnd.huawei.com/developer/boostkit/big-data

## 离线批处理特性

|  特性|特性介绍  |  
|--|--|
| [Omnioperator](https://gitee.com/openeuler/OmniOperator) | 采用C++结合向量化指令实现批处理算子，以列式内存数据格式OmniVec进行内存计算，充分利用向量化指令加速，提升SQL计算性能 |  
|[Gluten](https://gitee.com/openeuler/Gluten)  |基于Gluten为 Spark 引擎引入高性能的 Native Backend 支持  |  
|[OmniAdaptor](https://gitee.com/openeuler/OmniAdaptor)  | Spark 与 OmniOperator、Flink与OmniStream之间的桥梁组件，作为胶水层承接两者的运行时协作 |  

## 实时流处理在线加速特性
|  特性|特性介绍  |  
|--|--|
| [OmniStream](https://gitee.com/openeuler/OmniStream) | 采用C++结合向量化指令实现流式算子，以列式内存数据格式OmniVec进行内存计算，充分利用向量化指令加速，提升SQL计算性能 |  
|[OmniStateStore](https://gitee.com/openeuler/OmniStateStore)  |互联网搜推大数据场景会产生的超大状态十几~几十TB的数据，Flink原生的后端状态存储采用RocksDB，在处理超大状态时有读写性能较差和CPU占用较高等瓶颈点，进而影响到Flink的作业处理时长。BoostStateStore作为高性能的状态存储加速组件，使用内存和NvmeSSD磁盘来构建状态存储访问加速层，并且通过优化LSM-Tree的compaction流程来降低CPU抖动对Flink作业执行的影响。  |  


## 文档
- 鲲鹏BoostKit大数据文档
https://www.hikunpeng.com/document/detail/zh/kunpengbds/overview/kunpengbds.html