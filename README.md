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
2. [SparkContext常用函数入口介绍](./Pyspark/SparkContext入口函数.md)  
主要包括：
   - SparkContext编程入口
   - Accumulator累加器详解
   - addFile和SparkFiles加载本地文件，HDFS文件，http文件
   - binaryFiles读取二进制文件
   - Broadcast广播变量
   - setLogLevel日志级别
   - runJob使用
   - parallelize和range创建RDD详解
   - union的使用
   - statusTracker方法讲解
   - saveAsPickleFile和saveAsTextFile和其他压缩格式
3. [RDD算子介绍](./Pyspark/RDD算子.md)  
主要包括：
   - RDD算子的介绍
   - action算子
     - aggregate
     - collect
     - collectAsMap
     - count
     - countByKey
     - countByValue
     - first
     - fold
   - transformation算子
    
4. [SparkSQL编程入口](./Pyspark/SparkSQL编程入口SparkSession.md)
5. [SparkSQL模块简介](./Pyspark/SparkSQL模块简介.md)
6. [DataFrame创建的多种方式](./Pyspark/DataFrame创建的多种方式.md)
7. [DataFrame常用函数](./Pyspark/DataFrame.md)

#### Scala
1. 下面先总结常用算子的原理
- [<font size=+1>distinct算子</font>](./images/distinct算子原理.png)

2. Spark中间某些流程
- [spark任务执行流程](./images/spark任务流程.png)