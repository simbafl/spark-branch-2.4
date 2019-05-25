1. SparkContext上的broadcast方法用于创建广播变量，对于大于5M的共享变量，推荐使用广播。广播机制可以最大限度的减少网络IO，从而提升性能。

接下来，广播一个'hello'字符串，在各个task中接收广播变量，拼接返回。新建broadcast.py文件，内容如下：

```python
from Pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)
broad = context.broadcast('hello')
rdd = context.parallelize(np.arange(27), 3)

res = rdd.map(lambda x: str(x)+broad.value).collect()
print(res)
context.stop()

```
运行 spark-submit broadcast.py

2. defaultMinPartitions 获取默认最小的分区数
```python
from Pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)
p = context.defaultMinPattitions
print("defaultMinPartitions: ", p)
context.stop()

```

3. emptyRDD创建空RDD，没有数据，也没有分区
```bash
>>> sc = spark.sparkContext
>>> rdd = sc.emptyRDD()
>>> rdd.collect()
[]

```

4. getConf()方法返回作业的配置对象信息
```bash
>>> sc.getConf().toDebugString()
```

5. getLocalProperty和setLocalProperty获取和设置在本地线程中的属性信息，只对当前线程提交的作业有作用。
```bash
>>> sc.setLocalProperty('abc', 'hello')
>>> sc.getLocalProperty('abc')
u'hello'

```

6. setLogLevel设置日志级别，通过这个设置会将覆盖任何用户自定义的日志等级设置。取值有：ALL，DUBUG，ERROR，INFO， FATAL， OFF，WARN
```bash
>>> sc.setLogLevel('INFO')
```

7. getOrCreate得到或者是创建一个SparkContext对象，该方法创建的SparkContext对象为单例对象，该对象可以接收一个Sparkconf对象。
```bash
>>> sc1 = sc.getOrCreate()
>>> sc1 == sc
True

```