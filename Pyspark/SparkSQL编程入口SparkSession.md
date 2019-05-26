### SparkSQL编程入口SparkSession

1. 要编写SparkSQL程序，必须通过SparkSession对象。  
`pyspark.sql.SparkSession(sparkContext, jsparkSession=None) `

在spark1.x之前的版本中，SparkSQL程序的编程入口是：  
`pyspark.sql.SQLContext(sparkContext. sparkSession=None, jsqlContext=None)`  

这个是需要大家知道的，Spark1.x 和 Spark2.x在API方面变化还是很大的！

还有`pyspark.sql.HiveContext(sparkContext, jhiveContext=None)`，用于整合Hive数据仓库的，读取Hive表格需要借助 HiveContext。

2. 在SparkSession这个类中，有builder，通过builder去构建SparkSession实例，用法如下：
```shell
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.master('spark://hadoop-master:7077').appName('test').config('spark.xxx.conf', 'some-value').getOrCreate()

```

参数：
- master 用于指定spark集群地址，线上通常是`yarn`。
- appName用于设置app的名称。
- conf中以key，value的形式进行一些配置。
- conf可以链式编程的方式多次调用，每次调用可设置一组key，value配置。
- 而且conf中还可以传入一个关键字参数conf，指定外部的SparkConf配置对象getOrCreate，若存在sparksession实例直接返回，否则实例化一个sparksession返回。
- enableHiveSupport添加对Hive的支持。 