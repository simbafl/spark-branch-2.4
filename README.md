## 从源码开始剖析Spark
从事数据开发搞不定Spark，就犹如身在少林寺却不会铁头功。不学Spark一时爽，面试时却一直酸爽。目前我一直调用的是pyspark， 但是为了搞懂Spark还必须学点scala。接下来，让我们走进Spark的源码世界。

需要提前掌握的知识：
- Python
- Scala语法
- Hadoop原理
- Shell基础
- Sql基础
- Graph算法原理
- 机器学习原理

#### Python
1. [pyspark的模块介绍](./Pyspark/pyspark模块介绍.md)
2. [SparkContext编程入口以及Accumulator累加器详解](./Pyspark/SparkContext编程入口以及累加器.md)
3. [addFile加载文件和binaryFiles读取二进制文件](./Pyspark/addFile加载文件和binaryFiles读取二进制文件.md)
4. [Broadcast广播变量和setLogLevel日志级别](./Pyspark/Broadcast广播变量和setLogLevel日志级别.md)
5. [runJob和parallelize详解](./Pyspark/runJob和parallelize详解.md)
6. [union和statusTracker方法详解](./Pyspark/union和statusTracker方法详解.md)
7. [aggregate和aggregateByKey的异同及注意事项](./Pyspark/aggregate和aggregateByKey的异同及注意事项.md)
8. [collectAsMap和flod方法的理解以及正确使用](./Pyspark/collectAsMap和flod方法的理解以及正确使用.md)
9. [foreach和foreachPartitions原理以及使用场景和注意事项](./Pyspark/foreach和foreachPartitions原理以及使用场景和注意事项.md)
10. [histogram和lookup方法的使用详解](./Pyspark/histogram和lookup方法的使用详解.md)
11. [reduce和sampleStdev和saveAsPickleFile和saveAsTextFile和其他压缩格式](./Pyspark/reduce和sampleStdev和saveAsPickleFile和saveAsTextFile和其他压缩格式.md)

#### Scala
1. 下面先总结常用算子的原理
- [<font size=+1>distinct算子</font>](./images/distinct算子原理.png)

2. Spark中间某些流程
- [spark任务执行流程](./images/spark任务流程.png)