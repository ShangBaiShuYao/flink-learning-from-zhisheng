# Flink 精进学习知识星球内容整理

### 介绍



进知识星球的小伙伴有的是刚接触 Flink 的，有的是根本没接触过的![image](https://cdn.nlark.com/yuque/0/2020/png/311057/1596964608579-e55b42ad-54e1-4390-bdad-1c290b1476aa.png)，有的是已经用 Flink 很久的，所以很难适合所有的口味。



我一向认为对一门技术的学习方式应该是：



- 了解（知道它的相关介绍、用处）
- 用（了解常用 API）
- 用熟（对常用 API 能够用熟来，并了解一些高级 API）
- 解决问题（根据业务场景遇到的问题能够定位问题并解决）
- 看源码（深入源码的实现，此种情况主要是兴趣爱好驱动）



这里先把《从 0 到 1 学习 Flink》的系列文章给列出来，我觉得从这个系列文章的顺序来学习起码可以让你先达到第四个步骤，如果有什么疑问或者文章不足之处欢迎指出。



### 《从 0 到 1 学习 Flink》系列



- [Flink 从 0 到 1 学习 —— Apache Flink 介绍](http://www.54tianzhisheng.cn/2018/10/13/flink-introduction/) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2018/10/13/flink-introduction/) [Flink 架构、原理与部署测试](http://www.54tianzhisheng.cn/2019/06/14/flink-architecture-deploy-test/) 
- [Flink 从 0 到 1 学习 —— Mac 上搭建 Flink 1.6.0 环境并构建运行简单程序入门](http://www.54tianzhisheng.cn/2018/09/18/flink-install) 
- [Flink 从 0 到 1 学习 —— Flink 配置文件详解](http://www.54tianzhisheng.cn/2018/10/27/flink-config/) 
- [Flink 从 0 到 1 学习 —— Flink JobManager 高可用性配置](http://www.54tianzhisheng.cn/2019/01/13/Flink-JobManager-High-availability/) 
- [Flink 从 0 到 1 学习 —— Data Source 介绍](http://www.54tianzhisheng.cn/2018/10/28/flink-sources/) 
- [Flink 从 0 到 1 学习 —— 如何自定义 Data Source ？](http://www.54tianzhisheng.cn/2018/10/30/flink-create-source/) 
- [Flink 从 0 到 1 学习 —— Data Sink 介绍](http://www.54tianzhisheng.cn/2018/10/29/flink-sink/) 
- [Flink 从 0 到 1 学习 —— 如何自定义 Data Sink ？](http://www.54tianzhisheng.cn/2018/10/31/flink-create-sink/) 
- [Flink 从 0 到 1 学习 —— Flink Data transformation(转换)](http://www.54tianzhisheng.cn/2018/11/04/Flink-Data-transformation/) 
- [Flink 从 0 到 1 学习 —— 介绍Flink中的Stream Windows](http://www.54tianzhisheng.cn/2018/12/08/Flink-Stream-Windows/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2018/12/08/Flink-Stream-Windows/)[Flink 流计算编程--看看别人怎么用 Session Window](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239105&idx=1&sn=be0c2c5bd4396e94561e8aa08d98625c&chksm=8f5a1addb82d93cb184b782ac059c61230dc2f9b18604eb71542385647a094a21c30c5732447&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2018/12/08/Flink-Stream-Windows/)[这一次带你彻底搞懂 Flink Watermark](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239498&idx=2&sn=1b3d429f615d9bd9b0226f8737671546&chksm=8f5a1856b82d9140c6ccc90e8ef1aeb7654da8187122a6e1722225b305c2c5a0534eed6ae992&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 —— Flink 中几种 Time 详解](http://www.54tianzhisheng.cn/2018/12/11/Flink-time/) 
- [Flink 从 0 到 1 学习 —— Flink 项目如何运行？](http://www.54tianzhisheng.cn/2019/01/05/Flink-run/) 
- [Flink 从 0 到 1 学习 —— Flink parallelism 和 Slot 介绍](http://www.54tianzhisheng.cn/2019/01/14/Flink-parallelism-slot/) 
- [Flink 从 0 到 1 学习 —— Flink 写入数据到 ElasticSearch](http://www.54tianzhisheng.cn/2018/12/30/Flink-ElasticSearch-Sink/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2018/12/30/Flink-ElasticSearch-Sink/)[Flink 实时写入数据到 ElasticSearch 性能调优](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238504&idx=1&sn=7756ce7af39b2068eb72ced6f67cf992&chksm=8f5a0474b82d8d6202674cb5b6eafc35f5e006111bc94c19a900da12a5ce416c1731a924055a&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 —— Flink 写入数据到 Kafka](http://www.54tianzhisheng.cn/2019/01/06/Flink-Kafka-sink/) 
- [Flink 从 0 到 1 学习 —— Flink 读取 Kafka 数据批量写入到 MySQL](http://www.54tianzhisheng.cn/2019/01/15/Flink-MySQL-sink/) 
- [Flink 从 0 到 1 学习 —— Flink 读取 Kafka 数据写入到 RabbitMQ](http://www.54tianzhisheng.cn/2019/01/20/Flink-RabbitMQ-sink/) 
- [Flink 从 0 到 1 学习 —— 你上传的 jar 包藏到哪里去了?](http://www.54tianzhisheng.cn/2019/03/13/flink-job-jars/) 
- [Flink 从 0 到 1 学习 —— Flink 中如何管理配置？](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) 
- [Flink 从 0 到 1 学习—— 分享四本 Flink 国外的书和二十多篇 Paper 论文](http://www.54tianzhisheng.cn/2019/06/13/flink-book-paper/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[为什么说流处理即未来?](http://www.54tianzhisheng.cn/2019/06/15/Stream-processing/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[流计算框架 Flink 与 Storm 的性能对比](http://www.54tianzhisheng.cn/2019/06/17/flink-vs-storm/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[Flink Checkpoint 轻量级分布式快照](https://t.zsxq.com/QVFqjea) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[Flink状态管理和容错机制介绍](http://www.54tianzhisheng.cn/2019/06/18/flink-state/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[Apache Flink 结合 Kafka 构建端到端的 Exactly-Once 处理](http://www.54tianzhisheng.cn/2019/06/20/flink-kafka-Exactly-Once/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[使用 Prometheus Grafana 监控 Flink](https://t.zsxq.com/uRN3VfA)
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[使用 InflubDB 和 Grafana 监控 Flink JobManager TaskManager 和作业](https://t.zsxq.com/yVnaYR7)
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[从0到1搭建一套 Flink 监控系统](https://t.zsxq.com/vbIMJAu) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[详解 Flink Metrics 原理与监控实战](http://www.54tianzhisheng.cn/2019/11/23/flink-metrics/) 
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[Flink 读取 Kafka 商品数据后写入到 Redis](https://t.zsxq.com/RJqj6YV)
- [Flink 从 0 到 1 学习 —— ](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/)[一文搞懂 Flink 网络流控与反压机制](https://t.zsxq.com/ny3Z3rb) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [一文搞懂Flink内部的Exactly Once和At Least Once](https://t.zsxq.com/UVfqfae) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink On K8s](https://t.zsxq.com/eYNBaAa) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Apache Flink 是如何管理好内存的?](https://t.zsxq.com/zjQvjeM) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 参数配置和常见参数调优](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650241012&idx=1&sn=1fc2d3848c957f759036a5d2a55ae09f&chksm=8f5a1da8b82d94be63cbd12d4ceac54442b353f3d1453d02de72898e7b4af7e0f1affca1d568&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 状态生存时间（State TTL）机制的底层实现](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239999&idx=1&sn=5183e97f78d35b59cb6cdc318de114a7&chksm=8f5a19a3b82d90b59fe7cf9bf894f37245f722ed342fcc3a7fa046fe28cb8d4d6643c9c1cc84&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink State 最佳实践](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239366&idx=2&sn=7489bf383e2deb3921daf6480887e090&chksm=8f5a1bdab82d92cc6dcdf71ea51663b7d647f2259fe6e27b5244a66891c0156f6dfe233a7fe8&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 使用大状态时的一点优化](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239077&idx=2&sn=39bedd4f5a381f06a133acae290edd86&chksm=8f5a1a39b82d932fb1000ad1f45492b1b42c1eec3bb40ec40198bbc71d3e03b2134c2d242f7f&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 使用 broadcast 实现维表或配置的实时更新](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239265&idx=2&sn=cbc2bda736883cd9f695893a19e50544&chksm=8f5a1b7db82d926b20f0e2740303227da5b47bac2a661ccd61257c18b7d26b8207eab045abff&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Spark/Flink广播实现作业配置动态更新](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238830&idx=2&sn=8791a75eb00b50f3a687527fe9e10667&chksm=8f5a0532b82d8c2443cd6c0f9f0ddaff0be73cfda21a1db0a66cf7c586eed4f9b77bf3eb7b68&token=1858295303&lang=zh_CN#rd)
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 清理过期 Checkpoint 目录的正确姿势](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239009&idx=2&sn=2c63bcba31f91a15ca2e2cf3cacf5566&chksm=8f5a1a7db82d936bc6114dacfed17ca9ad4a92eab2c941ad35bfd90aefb67d35f741ace80563&token=1858295303&lang=zh_CN#rd)
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 状态管理与 Checkpoint 机制](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238962&idx=1&sn=b66c4940b1243fa74343650d1b3dc93a&chksm=8f5a05aeb82d8cb8453aee1435114a0fcfe50dc219fd1b3dfdf074d39f8ca6e7de802d5bae23&token=1858295303&lang=zh_CN#rd)
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 能否动态更改 Checkpoint 配置](http://www.54tianzhisheng.cn/2020/02/29/flink-nacos-checkpoint/)
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink Checkpoint 问题排查实用指南](http://www.54tianzhisheng.cn/2020/02/20/flink-checkpoint/) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Apache Flink 管理大型状态之增量 Checkpoint 详解](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238609&idx=2&sn=ad36eb812f8d487895300d0cfe2b1604&chksm=8f5a04cdb82d8ddb73c8624c20a7875f197f67bcf5569e1077f4df49107e494aa2c277beaae8&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [深入理解 Flink 容错机制](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238515&idx=2&sn=980e495c5373e64ef249c1ab1b6e072c&chksm=8f5a046fb82d8d79032fffb76cabdbcccf60518ec54451ae4f2db6a5a89ffb0ed3ffe8132e4c&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 使用 connect 实现双流匹配](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239239&idx=2&sn=8ea3e361350f7e5475d2f5acab24fe48&chksm=8f5a1b5bb82d924d8d3b091cd8c6f756f984de3fcec945f08d59973b01454e86c73684982c8c&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink流计算编程--Flink扩容、程序升级前后的思考](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239115&idx=2&sn=f6ff30687c0ecaf2e10b23434674257e&chksm=8f5a1ad7b82d93c1f2f492f70ea5257671eba44f48c00c1be8008310af15ad857c66fbbbc2cf&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink HDFS Sink 如何保证 exactly-once 语义](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239150&idx=2&sn=738a83a0c4981ac851c077d27fc390bb&chksm=8f5a1af2b82d93e42d24119e8563a6ec50968e5c3d9bce7f075777baff37f63583a169055a6d&token=1858295303&lang=zh_CN#rd)
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink Connector 深度解析](http://www.54tianzhisheng.cn/2019/08/24/Flink-Connector/) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [如何使用 Side Output 来分流？](http://www.54tianzhisheng.cn/2019/08/18/flink-side-output/) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 不可以连续 Split(分流)？](http://www.54tianzhisheng.cn/2019/06/12/flink-split/) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 全链路端到端延迟的测量方法](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238825&idx=1&sn=28da5840a8c22c7c675d7d4987824f33&chksm=8f5a0535b82d8c232681d2bc935bf99fa7c3d05f0406c184f71722e53a5209b2f19816f31ae1&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink on Yarn / K8s 原理剖析及实践](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238573&idx=2&sn=7d2488578fc93bfeaa58ddb4279925ce&chksm=8f5a0431b82d8d272250167dd261369a9ed824f94a1edc9ac49e73f16368b29b1e538bbfc62a&token=1858295303&lang=zh_CN#rd)
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [如何使用 Kubernetes 部署 Flink 应用](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238455&idx=2&sn=1b8cec83df9c72a4dbe56b11d6d3e7f0&chksm=8f5a07abb82d8ebddb349f2f48cae697a6a39a9bab16b80fba4850bb325ee2010c6df0cf8599&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [一张图轻松掌握 Flink on YARN 基础架构与启动流程](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238685&idx=2&sn=b023313ecbaf30d9a66e75636c6dfa7a&chksm=8f5a0481b82d8d970433c857c11a4e5af971f21d6c9c4e70eb619cfeb2b871a344b3a7764830&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink on YARN 常见问题与排查思路](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238700&idx=2&sn=a391c6cf1f1e4d6e22f6453a4033f575&chksm=8f5a04b0b82d8da68459f17ea2105b9f5f130b5aff498579e6cf4cb426cd6109a3a2462f1f5c&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink 单并行度内使用多线程来提高作业性能](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238695&idx=2&sn=222d820e2a0485ab31811b6ab774b0b2&chksm=8f5a04bbb82d8dad2650a4e751743d950a64082f2b7ba7796f8d35a936c076453b1cf3f0abbb&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink中资源管理机制解读与展望](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238477&idx=1&sn=fe053b103d62978274a6163110ee7b7a&chksm=8f5a0451b82d8d471d071e3260eb4acb20e7e3633d36bcd557f43d1065842305e183f0f9a0df&token=1858295303&lang=zh_CN#rd) 
- [Flink 从 0 到 1 学习 ——](http://www.54tianzhisheng.cn/2019/03/28/flink-additional-data/) [Flink Back Pressure(背压)是怎么实现的？有什么绝妙之处？](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238405&idx=1&sn=a262bef27509f0017688e16374ad44c0&chksm=8f5a0799b82d8e8f99a365822bc678b77ced5dc03c3c4817a4f8635d9b77e96e0ac258e88ffa&token=1858295303&lang=zh_CN#rd) 



  

### Flink SQL



- [知识星球 Flink 标签所有内容](https://wx.zsxq.com/dweb2/index/tags/Flink SQL/828214288542) 
- [Java SPI 机制在 Flink SQL 中的应用](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650240680&idx=1&sn=4046b105912524306ca4ceb4a598a251&chksm=8f5a1cf4b82d95e251b6caf14715d992d213219006b9bddb7883cdd203960fe6eaf5f796af47&token=1858295303&lang=zh_CN#rd) 
- [Flink 通过 DDL 和 SQL 来实现读取 Kafka 数据并处理后将数据写回 Kafka](https://t.zsxq.com/RJqj6YV)
- [Flink SQL 实战——读取Kafka数据处理后写入 ElasticSearch 6 和 7 两种版本](https://t.zsxq.com/RJqj6YV)
- [Flink 聚合性能优化 -- MiniBatch 分析](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239141&idx=2&sn=4b441f383f479215ebd5e1f5bb8a650f&chksm=8f5a1af9b82d93ef123a7789811551fe5d7d3e0360a7b48ead90befdbdf8b75ce7ad2cf1629c&token=1858295303&lang=zh_CN#rd) 
- [Flink流计算编程：双流中实现Inner Join、Left Join与Right Join](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239126&idx=1&sn=10b51d1409f5b3461294139aa5cc7e9e&chksm=8f5a1acab82d93dcd301ab695419ce829e7623c708d8cc8ed3099c7d00882f0211431b3c68e2&token=1858295303&lang=zh_CN#rd) 
- [Flink SQL 如何实现数据流的 Join？](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238819&idx=2&sn=97f1b2ddca379a28e390b21858bfc05b&chksm=8f5a053fb82d8c29881f4a788cd254322223cb39299c6211ec93bdccae13162dd023a38dea1f&token=1858295303&lang=zh_CN#rd) 







### 《Flink 各版本功能特性解读》



- [Apache Flink 1.9 重大特性提前解读](http://www.54tianzhisheng.cn/2019/07/01/flink-1.9-preview/) 
- [Flink 1.11 日志文件该如何配置？](https://t.zsxq.com/rFEUVBA) 
- [Flink 1.11 Release 文档解读](https://t.zsxq.com/a27yRJu)
- [Apache Flink 1.10 TaskManager 内存管理优化](https://t.zsxq.com/NNNVj2j) 
- [Flink 版本升级方案](https://t.zsxq.com/3BEemeq) 
- [Flink 1.11 新特性详解:【非对齐】Unaligned Checkpoint 优化高反压](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650240568&idx=2&sn=49a5e1418e6974584f404de03a6bc9d2&chksm=8f5a1c64b82d957287c1fefa6bf89a0da3c596c9b33f8c289e47aaf2c82d7aafeabe3785ccbe&token=1858295303&lang=zh_CN#rd) 
- [千呼万唤，Apache Flink 1.11.0 新功能正式介绍](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650240506&idx=2&sn=de3023a00224753985258156174f7103&chksm=8f5a1fa6b82d96b0592606cdf12ced520d108dac8c2bc867f9fb16ed1243cff3c8418cad68a8&token=1858295303&lang=zh_CN#rd) 
- [重磅！Apache Flink 1.11 会有哪些牛逼的功能](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239638&idx=1&sn=24f51ee03ca45304aa46ebeda2ac95b1&chksm=8f5a18cab82d91dcd8dd1d3953f2979ccc83f86256eacb286f8c6f52e76c185ca084ebbe6719&token=1858295303&lang=zh_CN#rd) 
- [Flink 1.10 新特性研究](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239034&idx=2&sn=484ee31a60a14245bff0c07260537398&chksm=8f5a1a66b82d93700626252ecab8766d19900543eb1e65c74ee9c99eb3c91d7f3fc944a90807&token=1858295303&lang=zh_CN#rd) 
- [修改代码150万行！Apache Flink 1.9.0做了这些重大修改！](http://www.54tianzhisheng.cn/2019/08/22/flink-1.9/) 





### 《Flink 在大厂的实践与应用》



- [OPPO 数据中台之基石：基于 Flink SQL 构建实时数据仓库](http://www.54tianzhisheng.cn/2019/06/16/flink-sql-oppo/) 
- [360深度实践：Flink与Storm协议级对比](http://www.54tianzhisheng.cn/2019/06/21/flink-in-360/) 
- [携程——如何基于Flink+TensorFlow打造实时智能异常检测平台？只看这一篇就够了](http://www.54tianzhisheng.cn/2019/06/26/flink-TensorFlow/) 
- [数据仓库、数据库的对比介绍与实时数仓案例分享](https://t.zsxq.com/v7QzNZ3) 
- [基于 Apache Flink 的监控告警系统 文章](https://t.zsxq.com/MniUnqb)
- [基于 Apache Flink 的监控告警系统 视频](http://www.54tianzhisheng.cn/2019/12/23/flink-monitor-alert/)
- [如何利用Flink Rest API 监控满足生产环境非常刚需的需求](https://t.zsxq.com/FMRNvr7)
- [无流量 Flink 作业告警](https://t.zsxq.com/NvnuRNj) 
- [Apache Flink 维表关联实战](https://t.zsxq.com/MRvzfAA) 
- [如何利用 Flink 实时将应用 Error 日志告警？](https://t.zsxq.com/2NbQFqF) 
- [Flink 流批一体的技术架构以及在阿里 的实践](https://t.zsxq.com/MvfUvzN) 
- [基于 Flink 搭建实时个性化营销平台？](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650240828&idx=2&sn=d05803fdf6f436fe1e61b6bc60eba375&chksm=8f5a1d60b82d94769b7e787773f82e7d339dd495d4d46bd8916585212cbbda145fbf99e731b9&token=1858295303&lang=zh_CN#rd) 
- [基于 Flink 和 Drools 的实时日志处理](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650240481&idx=1&sn=1f33c26869d87d0093232c5b70a0c02a&chksm=8f5a1fbdb82d96ab07611d1a18d12645b2b0c0b339e27338c9c7203045b06d838ae88f31ae1d&token=1858295303&lang=zh_CN#rd) 
- [新一代大数据实时数据架构到底长啥样](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239383&idx=1&sn=10933a52ddf0d7da1f8714076721c93a&chksm=8f5a1bcbb82d92dd6fac7130b8de1505d6333619f7f47fc85fda53825191a24f88b8ccc922b2&token=1858295303&lang=zh_CN#rd) 
- [从 Spark Streaming 到 Apache Flink：bilibili 实时平台的架构与实践](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239356&idx=1&sn=01af257bbe0f0ac4e55120fd223081bf&chksm=8f5a1b20b82d92361e759118362e832bfd30ff1a47530494122ee880a47f3faf7c49bc55ba4b&token=1858295303&lang=zh_CN#rd) 
- [日均万亿条数据如何处理？爱奇艺实时计算平台这样做](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239347&idx=1&sn=a5793f0e4f832ae6eb7d9583e1f4ec91&chksm=8f5a1b2fb82d92395ef6262c1217dbda64a1601103c6d3672a35d760f58148db0febcb62d7ae&token=1858295303&lang=zh_CN#rd) 
- [Flink 流批一体的实践与探索](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239313&idx=1&sn=ea9865bb3fe191ef0c1839638e46f866&chksm=8f5a1b0db82d921bae1a6cf70f008b7486f5f8f7d72f14e90f9ef0d00ba15b588d224340e732&token=1858295303&lang=zh_CN#rd) 
- [趣头条基于 Flink+ClickHouse 构建实时数据分析平台](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239311&idx=2&sn=8045b336e524067172ecd407ae35898a&chksm=8f5a1b13b82d920556ab98abe5a2c02edd3672299884b2ef99c8efde49465483854e9cf96d68&token=1858295303&lang=zh_CN#rd) 
- [Flink 维表关联多种方案对比](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239271&idx=1&sn=490e5ce1363fe73ef4b9b785bdaee46f&chksm=8f5a1b7bb82d926dcfcf9515ee36a28bbee67c3aa15490a2889ded545a68f37a12258348f53c&token=1858295303&lang=zh_CN#rd) 
- [美团点评基于 Flink 的实时数仓平台实践](http://www.54tianzhisheng.cn/2019/12/30/flink-meituan-real-time-warehouse/) 
- [基于 Apache Flink 的大规模准实时数据分析平台](http://www.54tianzhisheng.cn/2019/12/10/flink-real-time-data-analysis-platform/) 
- [阿里巴巴 Flink 踩坑经验：如何大幅降低 HDFS 压力？](http://www.54tianzhisheng.cn/2019/11/30/flink-checkpoint-hdfs/) 
- [58 同城基于 Flink 的千亿级实时计算平台架构实践](http://www.54tianzhisheng.cn/2019/11/30/flink-in-58/) 
- [基于 Flink 构建关联分析引擎的挑战和实践](http://www.54tianzhisheng.cn/2019/11/30/flink-aqniu/) 
- [滴滴实时计算发展之路及平台架构实践](http://www.54tianzhisheng.cn/2019/08/25/flink-didi/) 
- [如何使用 Flink 每天实时处理百亿条日志？](http://www.54tianzhisheng.cn/2019/08/23/flink-ebay/) 
- [美团点评基于 Flink 的实时数仓建设实践](http://www.54tianzhisheng.cn/2019/08/20/Flink-meituan-dw/) 
- [基于Kafka+Flink+Redis的电商大屏实时计算案例](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238820&idx=1&sn=652c34aff71827174011e999caaad037&chksm=8f5a0538b82d8c2e6203f921cf2fdd0ceab0bc54e9bdd2696036887ffc1cbeb6f8f25bfe0df1&token=1858295303&lang=zh_CN#rd) 
- [Flink 在小红书推荐系统中的应用](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238731&idx=1&sn=706a72f5ad2e4c3bb1ce22980886af46&chksm=8f5a0557b82d8c41312f88d1baf3feb1dca719805645938e2e1cc691402b18c65215d73ef43b&token=1858295303&lang=zh_CN#rd) 
- [Flink 实战 | 贝壳找房基于Flink的实时平台建设](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238450&idx=1&sn=109ecd71a60b1f96c6d8a020e08ab65e&chksm=8f5a07aeb82d8eb8bbd8bfb10febc977a42df7633cee79162c5300da0ce40ed93cabedb59b05&token=1858295303&lang=zh_CN#rd) 
- [Flink 在趣头条的应用与实践](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238345&idx=2&sn=f62f1dca57a5782bc3c68e63699ce4a4&chksm=8f5a07d5b82d8ec36f3931bb7da3fc24c4a2bc0ecf60b686cc3fb33c45eba02aca833c4a4cb9&token=1858295303&lang=zh_CN#rd) 







### 《Flink 实战与性能优化》专栏部分文章



有如下这些文章：



- [大数据重磅炸弹实时计算框架Flink](https://t.zsxq.com/UvrRNJM) 
- [《大数据重磅炸弹——实时计算引擎 Flink》开篇词](https://t.zsxq.com/fqfuVRR) 



**预备篇**：



- [你公司到底需不需要引入实时计算引擎？](https://t.zsxq.com/emMBaQN) 
- [一文让你彻底了解大数据实时计算框架 Flink](https://t.zsxq.com/eM3ZRf2) 
- [别再傻傻的分不清大数据框架Flink、Blink、Spark Streaming、Structured Streaming和Storm之间的区别了](https://t.zsxq.com/eAyRz7Y) 
- [Flink 环境准备看这一篇就够了](https://t.zsxq.com/iaMJAe6) 
- [一文讲解从 Flink 环境安装到源码编译运行](https://t.zsxq.com/iaMJAe6) 
- [通过 WordCount 程序教你快速入门上手 Flink](https://t.zsxq.com/eaIIiAm) 
- [Flink 如何处理 Socket 数据及分析实现过程](https://t.zsxq.com/Vnq72jY) 
- [Flink job 如何在 Standalone、YARN、Mesos、K8S 上部署运行？](https://t.zsxq.com/BiyvFUZ)  



**基础篇 :**



- [Flink 数据转换必须熟悉的算子（Operator)](https://t.zsxq.com/fufUBiA)  
- [Flink 中 Processing Time、Event Time、Ingestion Time 对比及其使用场景分析](https://t.zsxq.com/r7aYB2V)  
- [如何使用 Flink Window 及 Window 基本概念与实现原理](https://t.zsxq.com/byZbyrb)  
- [如何使用 DataStream API 来处理数据？](https://t.zsxq.com/VzNBi2r) 
- [Flink WaterMark 详解及结合 WaterMark 处理延迟数据](https://t.zsxq.com/Iub6IQf) 
- [Flink 常用的 Source 和 Sink Connectors 介绍](https://t.zsxq.com/IufaAU3)
- [Flink 最最最常使用的 Connector —— Kafka 该如何使用？](https://t.zsxq.com/ayB6miy)
- [如何自定义 Flink Connectors（Source 和 Sink）？](https://t.zsxq.com/qBAUji2)
- [Flink 读取 Kafka 数据后如何批量写入到 MySQL？](https://t.zsxq.com/n2rJAQR)
- [一文了解如何使用 Flink Connectors —— ElasticSearch？](https://t.zsxq.com/QRvJq7U)
- [一文了解如何使用 Flink Connectors —— HBase？](https://t.zsxq.com/aQVVvn2)
- [如何利用 Redis 存储 Flink 计算后的数据？](https://t.zsxq.com/vJ2V3rZ)



**进阶篇 :**



- [Flink State 详解及其如何如何选择 state？](https://t.zsxq.com/Z7amyBI)
- [如何选择 Flink 状态后端存储?](https://t.zsxq.com/ujeQ7eq)



### 《Flink 源码解析文章》



- [Flink 源码解析 —— 源码编译运行](https://t.zsxq.com/UZfaYfE)  
- [Flink 源码解析 —— 项目结构一览](https://t.zsxq.com/zZZjaYf)  
- [Flink 源码解析 —— ](https://t.zsxq.com/zZZjaYf)[Flink 源码的结构和其对应的功能点](https://t.zsxq.com/fEQN7Aa)
- [Flink 源码解析—— local 模式启动流程](https://t.zsxq.com/qnMFEUJ)  
- [Flink 源码解析 —— standalonesession 模式启动流程](https://t.zsxq.com/QZVRZJA) 
- [Flink 源码解析 —— Standalone Session Cluster 启动流程深度分析之 Job Manager 启动](https://t.zsxq.com/u3fayvf)  
- [Flink 源码解析 —— Standalone Session Cluster 启动流程深度分析之 Task Manager 启动](https://t.zsxq.com/MnQRByb)  
- [Flink 源码解析 —— 分析 Batch WordCount 程序的执行过程](https://t.zsxq.com/YJ2Zrfi) 
- [Flink 源码解析 —— 分析 Streaming WordCount 程序的执行过程](https://t.zsxq.com/qnMFEUJ) 
- [Flink 源码解析 —— 如何获取 JobGraph？](https://t.zsxq.com/naaMf6y) 
- [Flink 源码解析 —— 如何获取 StreamGraph？](https://t.zsxq.com/qRFIm6I) 
- [Flink 源码解析 —— Flink JobManager 有什么作用？](https://t.zsxq.com/2VRrbuf) 
- [Flink 源码解析 —— Flink TaskManager 有什么作用](https://t.zsxq.com/RZbu7yN)  
- [Flink 源码解析 —— JobManager 处理 SubmitJob 的过程](https://t.zsxq.com/3JQJMzZ) 
- [Flink 源码解析 —— TaskManager 处理 SubmitJob 的过程](https://t.zsxq.com/eu7mQZj) 
- [Flink 源码解析 —— 深度解析 Flink Checkpoint 机制](https://t.zsxq.com/ynQNbeM) 
- [Flink 源码解析 —— 深度解析 Flink 序列化机制](https://t.zsxq.com/JaQfeMf)  
- [Flink 源码解析 —— 深度解析 Flink 是如何管理好内存的？](https://t.zsxq.com/zjQvjeM) 
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-core 源码解析](https://t.zsxq.com/Mnm2nI6)  
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-datadog 源码解析](https://t.zsxq.com/Mnm2nI6)  
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-dropwizard 源码解析](https://t.zsxq.com/Mnm2nI6)  
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-graphite 源码解析](https://t.zsxq.com/Mnm2nI6) 
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-influxdb 源码解析](https://t.zsxq.com/Mnm2nI6) 
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-jmx 源码解析](https://t.zsxq.com/Mnm2nI6) 
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-slf4j 源码解析](https://t.zsxq.com/Mnm2nI6)  
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-statsd 源码解析](https://t.zsxq.com/Mnm2nI6)  
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink-metrics-prometheus 源码解析](https://t.zsxq.com/Mnm2nI6)  
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink 注解源码解析](https://t.zsxq.com/Mnm2nI6) 
- [Flink 源码解析 ——](https://t.zsxq.com/zjQvjeM) [Flink Metrics 实战](https://t.zsxq.com/Mnm2nI6) 





### 《Flink 自己录制过的视频》



- [Flink 整合 Apollo 动态更新配置](https://t.zsxq.com/RJqj6YV)
- [Flink 整合 Nacos 动态更新配置](https://t.zsxq.com/RJqj6YV)
- [Flink 专栏的开篇词](https://t.zsxq.com/RJqj6YV) 
- [你公司到底需不需要引入实时计算引擎](https://t.zsxq.com/RJqj6YV)
- [一文让你彻底了解大数据实时计算框架 Flink](https://t.zsxq.com/RJqj6YV)
- [别再傻傻的分不清大数据框架 Flink、Blink、Spark Streaming、Structured Streaming 和 Storm 之间的区别了](https://t.zsxq.com/RJqj6YV)
- [Flink环境准备](https://t.zsxq.com/RJqj6YV)
- [Flink环](https://t.zsxq.com/RJqj6YV)[境](https://t.zsxq.com/RJqj6YV)[安装](https://t.zsxq.com/RJqj6YV)
- [Flink WordCount 程序入门上手及分析实现过程](https://t.zsxq.com/RJqj6YV)
- [Flink 如何处理 Socket 数据及分析实现过程](https://t.zsxq.com/RJqj6YV)
- [Flink 中 Processing Time、Event Time、Ingestion Time 对比及其使用场景分析](https://t.zsxq.com/RJqj6YV)
- [如何使用 Flink Window 及 Window 基本概念与实现原理](https://t.zsxq.com/RJqj6YV)
- [Flink_Window组件深度讲解和如何自定义Window](https://t.zsxq.com/RJqj6YV)
- [Flink 读取 Kafka 商品数据后写入到 Redis](https://t.zsxq.com/RJqj6YV)
- [基于 Apache Flink 的监控告警系统](https://t.zsxq.com/RJqj6YV)
- [Flink源码解析01——源码编译运行](https://t.zsxq.com/RJqj6YV)
- [Flink源码解析02——源码结构一览](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析03——源码阅读规划](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析04——flink-example模块源码结构](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析05——flink-example模块源码分析](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析06——flink-example-streaming 异步IO源码分析](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析07——flink-example-streaming SideOutput源码分析](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析08——flink-example-streaming Socket源码分析](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析09——flink-example-streaming window和join源码分析](https://t.zsxq.com/BaIQBqr)
- [Flink源码解析10——flink-example-streaming 源码分析总结](https://t.zsxq.com/BaIQBqr)
- [Flink到底是否可以动态更改checkpoint配置](https://t.zsxq.com/RJqj6YV)
- [Flink 通过 DDL 和 SQL 来实现读取 Kafka 数据并处理后将数据写回 Kafka](https://t.zsxq.com/RJqj6YV)
- [Flink SQL 实战——读取Kafka数据处理后写入 ElasticSearch 6 和 7 两种版本](https://t.zsxq.com/RJqj6YV)







### 其他资源下载



- [Flink Forward Asia 2019 的 PPT和视频下载](http://www.54tianzhisheng.cn/2019/12/07/Flink_Forward_Asia_2019/)
- [Flink Forward 2020 PPT 下载](http://www.54tianzhisheng.cn/2020/05/13/flink-forward-2020/) 
- [实时计算平台架构（上）](https://t.zsxq.com/emuVZrb)
- [实时计算平台架构（下）](https://t.zsxq.com/UnmMZN7)
- [基于Flink实现的商品实时推荐系统](https://t.zsxq.com/VfQ76iU)
- [Flink1.8学习路线](https://t.zsxq.com/jIYB6QR)
- [Kafka 学习文章和视频](https://t.zsxq.com/6Q3vN3b) 
- [数据分析指南](https://t.zsxq.com/UNvnae6) 
- [TimeoutException The heartbeat of TaskManager](https://t.zsxq.com/jei27QZ) 
- [Flink on RocksDB 参数调优指南](https://t.zsxq.com/nu3fyV7) 
- [2020最新Java面试题及答案](https://t.zsxq.com/RRvjAyn) 
- [以业务为核心的中台体系建设](https://t.zsxq.com/vBuzVj6) 
- [Skip List--跳表(全网最详细的跳表文章没有之一)](https://t.zsxq.com/Q7uVrvz) 
- [Stream Processing with Apache Flink](https://t.zsxq.com/N37mUzB) 
- [假如我是面试官，我会问你这些问题，请接招](https://t.zsxq.com/NZf2JAq) 
- [YARN 运行机制分析](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239970&idx=2&sn=8703de96f1657bd811953dfff4d6abdd&chksm=8f5a19beb82d90a8949b3bdd0c517ae66becede295307d6e3d7f8e634b82a2972797ef8648b9&token=1858295303&lang=zh_CN#rd) 
- [企业大数据平台仓库架构建设思路](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650239168&idx=1&sn=2e3e8d26b3eec376782b4415be86b399&chksm=8f5a1a9cb82d938afc6c842187b255120624bfaa7c8090a9554277f08f279c28500d1191dd8a&token=1858295303&lang=zh_CN#rd) 
- [阿里巴巴开源的 Blink 实时计算框架真香](http://www.54tianzhisheng.cn/2019/02/28/blink/) 
- [吐血之作 | 流系统Spark/Flink/Kafka/DataFlow端到端一致性实现对比](https://mp.weixin.qq.com/s?__biz=MzIxMTE0ODU5NQ==&mid=2650238519&idx=1&sn=7967857af5e7c435715c388592e326f9&chksm=8f5a046bb82d8d7d15c678d21e274301183a446911dd66739a8728aa6794033eb6cea7d62ea5&token=1858295303&lang=zh_CN#rd) 







另外就是星球里可以向我提问，我看到问题会及时回答的，发现提问的还是比较少，想想当初就该还是要所有的都付费才能进，免费进的就会让你不珍惜自己付出的钱💰，自己也不会持续跟着一直学习下去。后面我会根据提问情况把长期潜水且当初是没付费的移除掉！



还有就是群里的一些问题解答会同步到这里沉淀下来！如果你对这些问题还有更好的解答也欢迎提出你的回答，如果觉得棒的话我会进行一定额度的打赏！



打赏包括但不限制于：



- 高质量的问题
- 学习资料资源分享
- 问题全面的解答
- 分享自己的建议



好好做好这几点，肯定会把入知识星球的钱赚到！



为什么要做一个这样的 Flink 知识星球？



- 帮助他人成长就是自己在成长
- 主动促使自己去深入这门技术（心里总觉得要对得起付费玩家）
- 真的想遇到那么一两个人可以一直好好学习下去（学习真特么是孤独的，一个人学习确实遇到的坑很多，效率肯定也低点，如果没有找到的话，那么还是我自己的那句话：坑要自己一个个填，路要自己一步步走！）



![image.png](https://cdn.nlark.com/yuque/0/2020/png/311057/1596977462206-50305ec5-1fe3-485e-856f-682575495997.png)



一个人走的快一些，一群人走的远一些，欢迎扫码上面的二维码加入知识星球，我们一起向前！