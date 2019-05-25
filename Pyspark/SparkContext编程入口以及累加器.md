1. SparkContext是pyspark的编程入口，作业的提交，任务的分发，应用的注册都会在SparkContext中进行。一个SparkContext实例代表着和Spark的一个连接，只有建立了连接才可以把作业提交到集群中去。实例化了SparkContext之后才能创建RDD和Broadcast广播变量。

2. SparkContext获取，启动 pyspark -master local 之后，可以通过SparkSession获取SparkContext对象。

   另一种获取方式是，引入pyspark.SparkContext进行创建。
   
sparkContext.py
```python
from Pyspark import SparkContext
from Pyspark import SparkConf

conf = SparkConf()
conf.set('master', 'local')
sparkContext = SparkContext(conf = conf)
rdd = sparkContext.parallelize(range(100))
print(rdd.collect())
sparkContext.stop()
```
然后执行 spark-submit sparkContext.py

3. accumulator是SparkContext上用来创建累加器的方法。创建累加器可以在各个task中进行累加，并且只能够自持add操作。该方法支持传入累加器的初始值。

accumulator.py
```python
from Pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)
acc = context.accumulator(0)  # 累加器从0开始
print(type(acc), acc.value)
rdd = context.parallelize(np.arange(101), 5)
def acc_add(a):
    acc.add(a)
    return a
    
rdd2 = rdd.map(acc_add)  # 对所有rdd进行累加
print(rdd2.collect())  # action触发作业的提交
print(acc.value)
context.stop()
```
然后执行 spark-submit accumulator.py