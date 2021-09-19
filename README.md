## 从源码剖析Spark

2021年入手M1 Pro，打算体验一遍，于是给windows重装了系统。结果编译spark十几次不成功，最后被回复mac版本太新某些包存在bug。
我就欲哭无泪，又被打回到了Windows。

**版本：2.4.0**
**完成时间：2021-08-27**

- [spark编译](./images/spark编译.png)


### pyspark
1. [pyspark的模块介绍](./Pyspark/pyspark模块介绍.md)


### Spark
2. [SparkContext常用函数入口介绍](./Pyspark/SparkContext入口函数.md)  
3. [RDD算子介绍](./Pyspark/RDD算子.md)  
4. [SparkSQL编程入口](./Pyspark/SparkSQL编程入口SparkSession.md)
5. [SparkSQL模块简介](./Pyspark/SparkSQL模块简介.md)
6. [DataFrame创建的多种方式](./Pyspark/DataFrame创建的多种方式.md)
7. [DataFrame常用函数](./Pyspark/DataFrame.md)

#### 流程图
1. 常用算子
- [<font size=+1>distinct算子</font>](./images/distinct算子原理.png)

2. Spark处理流程
- [spark任务执行流程](./images/spark任务流程.png)