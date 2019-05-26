### 创建DataFrame的几种方式  

1. createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)  
可以使用RDD或python list获取pandas.DataFrame来创建。  
当schema被指定时，每个列的类型将会通过列来判断。  
samplingRatio指定用于类型推断的样本的比例。  
verifySchema验证每行的数据类型是否符合schema的定义。

2. 使用二值tuple创建DataFrame，注意有schema和没有指定schema的区别。  
```shell
>>> l = [('Marry', 20), ('Bob', 24)]
>>> spark.createDataFrame(l).collect()
>>> spark.createDataFrame(l, schema=['name', 'age']).collect()

```

3. 使用schema创建DataFrame，字典中的key就是schema
```shell
>>> l = [{'name': 'Marry', 'age': 20}, {'name': 'Bob', 'age': 24}]

```

4. 通过RDD来创建DataFrame
``` 
>>> l = [('Tom', 14), ('Jerry', 19)]
>>> rdd = sc.parallelize(l, 3)
>>> df = spark.createDataFrame(rdd)
>>> df1 = spark.createDatarame(rdd, schema=['name', 'age'])

```

5. 通过在RDD上构建Row对象来构建DataFrame
```python
from Pyspark.sql import Row

Person = Row('name', 'age')
l = [('Tom', 15), ('Bob', 19)]
rdd = sc.parallelize(l, 3)
person = rdd.map(lambda r: Person(*r))  # 此时person由Row对象组成
df = spark.createDataFrame(person)

```

6. 通过传入RDD额schema信息来构建DataFrame
```python
from pyspark.sql.types import *

schema = StructType([StructField('name', StringType(), True), StructField('age', IntegerType(), True)])
l = [('Bob', 16), ('Tom', 20)]
rdd = sc.parallelize(l, 3)
df = spark.createDataFrame(rdd, schema)

```

7. 通过Pandas中的DataFrame来创建DataFame
```python
import pandas as pd
pandas_df = pd.DataFrame([{'name': 'Tom', 'age': 14}, {'name': 'jerry', 'age': 20}])
pandas_df.head()
spark.createDataFrame(pandas_df).collect()

```

8. schema 还可以用字符串表示
```shell
l = [('Tom', 15), ('Bob', 19)]
rdd = sc.parallelize(l, 3)
spark.createDataFrame(rdd, 'name:string, age:int').collect()

```