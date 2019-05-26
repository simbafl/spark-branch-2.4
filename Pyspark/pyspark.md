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

4. testFile和saveAsTextFile读取位于hdfs上的文本文件。
```bash
>>> rdd = sc.textFile('/data/num_data', 3)
>>> rdd.collect()
[u'100']

```

5. parallelize使用python集合创建RDD，可以使用range函数，当然也可以使用numpy中arange方法来创建。
```python
rdd = sc.parallelize(range(10))

import numpy as np
rdd = sc.parallelize(np.arange(10), 4)

```

6. saveAsPickleFile和pickleFile将RDD保存为python中pickle压缩文件格式。
```bash
>>> sc.parallelize(range(100), 3).saveAsPickleFile('/data/pickle/t', 5)
>>> sorted(sc.pickleFile('/data/pickle/t', 3).collect(), reverse=True)

```

7. range(start, end=None, step=1, numSlices=None) 按照提供的起始值和步长，创建RDD。numSlices指定分区数。
```bash
>>> rdd = sc.range(1, 100, 11, 3)
>>> rdd.getNumPartitions()
3

```

8. runJob(rdd, partitionFunc, partitions=None, allowLocal=False)在给定的分区上运行指定的函数partitions用于指定分区的编号，是一个列表。若不指定分区默认为所有分区运行partitionFunc函数。
```bash
>>> rdd = sc.range(1, 1000, 11, 10)
>>> sc.runJob(rdd, lambda x: [a**2 for a in x], partitons=[1,3,5])

```

9. setCheckpointDir(dirName) 设置检查点的目录，检查点用于异常发生时错误的恢复，该目录必须为HDFS目录。

```bash
>>> sc.setCheckpointDir('/data/checkpoint/')
>>> rdd = sc.range(1, 1000, 11, 10)
>>> rdd.checkpoint()
>>> rdd.collect()

```

10. sparkUser获取运行当前作业的用户名
```bash
>>> sc.sparkUser()
u'root'

```

11. startTime 返回作业启动的时间，Long类型的毫秒时间值
```bash
>>> sc.startTime
1519376964085L
```

12. addFile方法添加文件，使用`SparkFiles.get`方法获取文件
这个方法接收一个路径，该方法会把本地路径下的文件上传到集群中，以供运算过程中各个node节点下载数据，路径可以是本地路径也可以是hdfs路径，或者一个http，https或者ftp的url。如果上传的是一个文件夹，则指定recursize参数为True。上传的文件使用`SparkFiles.get(filename)`的方式进行获取。

访问本地路径文件 
新建addFile.py文件，内容如下：
```python
from Pyspark import SparkFiles
import os
import numpy as np
from Pyspark import SparkContext
from Pyspark import SparkConf

tempdir = '/root/workspace'
path = os.path.join(tempdir, "num_data")
with open(path, "w") as f:
    f.write('1000')
conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)
context.addFile(path)  # 把path加入到集群
rdd = context.parallelize(np.arange(10))
def fun(iterable):
    with open(SparkFiles.get('num_data')) as f:
        value = int(f.readline())
        return [x*value for x in iterable]
        
print(rdd.maPartitions(fun).collect())
context.stop()

```
运行 spark-submit addFile.py


访问 hdfs文件  
新建hdfs_addFile.py文件，内容如下：
```python
from Pyspark import SparkFiles
import numpy as np
from Pyspark import SparkContext
from Pyspark import SparkConf

conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)
path = 'hdfs://hadoop-master:9000/datas/num_data'
context.addFile(path)
rdd = context.parallelize(np.arange(10))
def fun(iterable):
    with open(SparkFiles.get('num_data')) as f:
        value = int(f.readline())
        return [x*value for x in iterable]
        
print(rdd.maPartitions(fun).collect())
context.stop()

```
运行 spark-submit hdfs_addFile.py


访问http网络文件  
其他都一致，只需修改path，如下：
```python
path = 'http://192.168.0.6:8080/num_data'
context.addFile(path)
```

13. applicationId，用于获取注册到集群的应用的id
```python
from Pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)
rdd = context.parallelize(np.arange(10))
print('applicationid: ', context.applicationId)
print(rdd.collect())
```
结果如下：
```shell
>>> ('applicationid: ', u'app-20190522135159-0006')
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

14. binaryFiles方法用于读取二进制文件例如音频、视频、图片，对于每个文件器返回一个tuple，tuple第一个参数为文件路径，第二个路径为二进制文件的内容。

新建 binaryFiles.py文件，读取图片，内容如下：
```python
from Pyspark import SparkContext, SparkConf
import numbers as np

conf = SparkConf()
conf.set('master', 'local')
context = SparkContext(conf=conf)

rdd = context.binaryFiles('/data/pic/')
print('applicationId: ', context.applicationId)
result = rdd.collect()
for data in result:
    print(data[0], data[1][:10])
context.stop()

```
运行 spark-submit binaryFiles.py

15. SparkContext上的broadcast方法用于创建广播变量，对于大于5M的共享变量，推荐使用广播。广播机制可以最大限度的减少网络IO，从而提升性能。

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

16. defaultMinPartitions 获取默认最小的分区数
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

17. emptyRDD创建空RDD，没有数据，也没有分区
```bash
>>> sc = spark.sparkContext
>>> rdd = sc.emptyRDD()
>>> rdd.collect()
[]

```

18. getConf()方法返回作业的配置对象信息
```bash
>>> sc.getConf().toDebugString()
```

19. getLocalProperty和setLocalProperty获取和设置在本地线程中的属性信息，只对当前线程提交的作业有作用。
```bash
>>> sc.setLocalProperty('abc', 'hello')
>>> sc.getLocalProperty('abc')
u'hello'

```

21. setLogLevel设置日志级别，通过这个设置会将覆盖任何用户自定义的日志等级设置。取值有：ALL，DUBUG，ERROR，INFO， FATAL， OFF，WARN
```bash
>>> sc.setLogLevel('INFO')
```

22. getOrCreate得到或者是创建一个SparkContext对象，该方法创建的SparkContext对象为单例对象，该对象可以接收一个Sparkconf对象。
```bash
>>> sc1 = sc.getOrCreate()
>>> sc1 == sc
True

```

