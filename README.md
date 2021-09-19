# Apache Spark

## 基础设施
1. SparkConf - core.scala.SparkConf
2. RPC框架【对Netty API的封装】- common.network-common.java.org.apache.spark.network
3. 事件总线【采用监听器模式实现事件与监听器】- core.scala.org.apache.spark.scheduler
4. 度量系统【依赖Metrics API】- core.scala.org.apache.spark.metrics

## SparkContext
1. SparkEnv：Spark运行环境。Driver[local]和Executor都会以来SparkEnv提供的环境。其内部包含了很多组件，
   例如，RpcEnv、BlockManager、mapOutputTracker等。
2. LiveListenerBus：事件总线。接收各个使用方的事件，同时以异步的方式对事件进行匹配后调用SparkListener的不同方法。
3. SparkUI：SparkUI间接依赖于计算引擎、调度系统、存储体系，Job、Stage、存储、Executor等组件的监控数据都会
   以SparkListenerEvent的形式投递到LiveListenerBus。
4. SparkStatusTracker：提供对作业、Stage等的监控信息。
5. ConsoleProgressBar：利用SparkStatusTracker的API，在控制台展示Stage的进度。
6. DAGScheduler：DAG调度器。负责创建Job，将DAG中的RDD划分到不同的Stage、提交Stage等。
   SparkUI中有关Job和Stage的监控数据都来自DAGScheduler。
7. TaskScheduler：任务调度器。TaskScheduler按照调度算法对集群管理器已经给应用程序的资源进行二次调度，分配给任务。
   TaskScheduler是由DAGScheduler创建。
8. HeartbeatReceiver：心跳接收器。所有Executor都会向HeartbeatReceiver发送心跳信息，HeartbeatReceiver接收到
   Executor的心跳信息后，首先更新Executor的最后可见时间，然后将此信息交由TaskScheduler作进一步处理。
9. ContextCleaner：上下文清理器。ContextCleaner用异步的方式清理那些超过应用域范围的RDD、ShuffleDependency和Broadcast。
10. JobProgressListener：作业进度监听器。注册到LiveListenerBus作为监听器之一使用。
11. ShutdownHookManager：顾名思义，用于设置关闭钩子的管理器。可以在关闭前做一些清理工作。

## SparkEnv
1. SecurityManager：安全管理器
2. RPCEnv：RPC环境【长篇大论的Netty】
3. SerializerManager：序列化管理器
4. BroadcastManager：广播管理器
5. mapOutputTracker：map任务输出跟踪器
6. 存储体系
   - ShuffleManager【Shuffle管理器】
   - MemoryManager【内存管理器】
   - BlockTransferService【块传输管理器】
   - BlockManagerMaster【BlockManager管理器】
   - DiskBlockManager【磁盘块管理器】
   - BlockInfoManager【块锁管理器】
   - BlockManager【块管理器】
7. MetricsSystem：度量系统
8. OutputCommitCoordinator：输出提交协调器【有任务需要输出到HDFS】