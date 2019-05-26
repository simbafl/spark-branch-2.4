### RDD算子

1. RDD是Spark里面最重要的基础抽象，代表的是弹性的分布式的数据集。RDD有很多的实现类，在各个RDD之上提供了transformation和action两大类算子。transformation算子具有惰性，他们并不会触发作业的提交，一个个的transformation算子操作只是定义出了计算所依赖的DAG有向无环图，它只是一个计算的逻辑，而真正会触发作业提交的算子是属于action类别的算子。接下来总结下最常用到的算子。

2. spark中如何实现分布式并行计算一个一个数据集中所有元素的和？这属于一个聚合操作，spark先把数据分区，每个分区由任务调度器分发到不同的节点分别计算和，计算的结果返回driver节点，在driver节点将所有节点的返回值相加，就得到了整个数据集的和了。
spark里实现了这样一个算子aggregate(zeroValue, seqOp, combOp)。zeroValue是每个节点相加的初始值，seqOp为每个节点上调用的运算函数，combOp为节点返回结果调用的运算函数。

```shell
>>> sc = spark.sparkContext
>>> import numpy as np
>>> rdd = sc.parallelize(np.arange(11), 3)
>>> rdd.collect()
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
>>> rdd.aggregate(0, lambda x,y: x+y, lambda x,y: x+y)
55
>>> rdd.aggregate(8, lambda x,y: x+y, lambda x,y: x+y)
87
```
从结果看到每个分区上调用seqOp函数都要加上zeroValue，最后运行combOp也要加上zeroValue。3个分区加上最后的combOp，所以总共加了四次zeroValue。

3. aggregateByKey(zeroValue, seqFunc, combFunc, numPartitions=None, partitionFunc=<fun_hash>)。这个方法用来对相同的key值进行聚合操作，同样的是指定zeroValue,seqFunc,numPartitions为分区数，partitionFunc为作用在分区上的函数。这个方法和aggregate方法很相似，但是它却是一个transformation方法，不会触发作业的提交！
```shell
>>> datas = [('a', 11), ('b', 22), ('c', 33), ('a', 4), ('b', 12)]
>>> rdd = sc.parallelize(datas, 3)
>>> rdd.collect()
[('a', 11), ('b', 22), ('c', 33), ('a', 4), ('b', 12)]
>>> rdd.aggregateByKey(0, lambda x,y: x+y, lambda x,y: x+y)
PythonRDD[126] at RDD at PythonRDD.scala:48
>>> rdd.aggregateByKey(0, lambda x,y: x+y, lambda x,y: x+y).collect()
[('a', 15), ('b', 34), ('c', 33)]

```

4. collect方法触发作业的提交，返回一个结果的列表。注意：若结果集较大，使用collect方法可能使driver程序崩溃，因为collect方法会返回所有节点的结果数据到driver节点，造成OOM异常。

5. collectAsMap方法也是action方法，会触发作业的执行，顾名思义该方法返回的结果是一个字典结构。
```shell
>>> rdd1 = rdd.collectAsMap()
>>> type(rdd1)
<type 'dict'>
>>> rdd1
{'a':15, 'b':34, 'c': 33}
中元素的个数
```

6. count方法，统计RDD

7. countApprox(timeout, confidence=0.95)
带有超时限制的count统计函数，时间一到，即便所有任务还没有完成，该方法也会返回已经完成的任务的统计结果。

8. countApproxDistinct方法返回大概的RDD数据集中没有重复数据的数据条纹。

9. countByKey方法统计相同key的个数，以字典形式返回。
```shell
>>> data = [('a', 11), ('b', 12), ('c', 13), ('b', 14)]
>>> rdd = sc.parallelize(data, 3)
>>> rdd.countByKey()
defaultdict(<type 'int'>, {'a':1, 'b':2, 'c':1})

```

10. countByValue方法统计值出现的次数，以值为键，次数为值返回字典。
```shell
>>> data = ['a', 'b', 'c', 'a', 'b', 'c']
>>> rdd = sc.parallelize(data, 3)
>>> rdd.countByValue()
defaultdict(<type 'int'>, {'a':3, 'b':2, 'c':1})

```

11. first方法返回RDD中第一条记录。
```shell
>>> rdd = sc.parallelize(range(10))
>>> rdd.first()
0

```

12. fold方法使用给定的zeroValue和op方法，先聚合每一个partitions中的记录，然后全局聚合。
```shell
>>> rdd = sc.parallelize(range(11), 3)
>>> rdd.fold(0, lambda x,y: x+y)
55
>>> rdd.fold(1, lambda x,y: x+y)
59

```
该方法类似于aggragate，但是aggragate方法需要提供两个op，fold方法只需要一个即可！

 