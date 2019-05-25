1. addFile方法添加文件，使用`SparkFiles.get`方法获取文件
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

2. applicationId，用于获取注册到集群的应用的id
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

3. binaryFiles方法用于读取二进制文件例如音频、视频、图片，对于每个文件器返回一个tuple，tuple第一个参数为文件路径，第二个路径为二进制文件的内容。

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
