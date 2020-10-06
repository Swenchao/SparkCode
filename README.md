# SparkCode

学习Spark所写demo实践

博客会定时更新Spark学习笔记——https://swenchao.github.io/ 

小白学习笔记，多包涵~

## 文件说明

### SparkCore

1. WordCount案例

[com.swenchao.spark.core.WordCount](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/WordCount.scala)

2. 生成RDD方式（从集合中创建RDD）

[com.swenchao.spark.core.Spark01_RDD](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark01_RDD.scala)

3. map案列（创建一个1-10数组的RDD，将所有元素*2形成新的RDD）

[com.swenchao.spark.core.Spark02_Oper1](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark02_Oper1.scala)

4. mapPartitions案列（创建一个1-10数组的RDD，将所有元素*2形成新的RDD）

[com.swenchao.spark.core.Spark03_Oper2](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark03_Oper2.scala)

5. mapPartitionsWithIndex案例（创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD）

[com.swenchao.spark.core.Spark04_Oper3](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark04_Oper3.scala)

6. flatMap案例（创建一个元素为1-4的RDD，运用flatMap创建一个新的RDD，将所有数字分开）

[com.swenchao.spark.core.Spark05_Oper4](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark05_Oper4.scala)

7. glom案例（创建一个4个分区的RDD，并将每个分区的数据放到一个数组）

[com.swenchao.spark.core.Spark06_Oper5](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark06_Oper5.scala)

8. groupBy案例（创建一个RDD，按照元素模以2的值进行分组）

[com.swenchao.spark.core.Spark07_Oper6](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark07_Oper6.scala)

9. filter案例（创建一个RDD（1 2 3 4），过滤出一个新RDD（%2为0的））

[com.swenchao.spark.core.Spark08_Oper7](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark08_Oper7.scala)

10. sample(withReplacement, fraction, seed) 案例（创建一个RDD（1-10），从中选择放回和不放回抽样）

[com.swenchao.spark.core.Spark09_Oper8](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark09_Oper8.scala)

11. distinct案例（创建一个RDD，使用distinct()对其去重）

[com.swenchao.spark.core.Spark10_Oper9](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark10_Oper9.scala)

12. coalesce案例（创建一个4个分区的RDD，对其缩减分区）

[com.swenchao.spark.core.Spark11_Oper10](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark11_Oper10.scala)

13. 自定义分区器案例（创建一个List(("a", 1), ("b", 2), ("c", 3))的RDD，使用自定义分区器进行分区）

[com.swenchao.spark.core.Spark12_Oper11](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark12_Oper11.scala)

14. aggregateByKey案例（创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加）

[com.swenchao.spark.core.Spark13_Oper12](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark13_Oper12.scala)

15. foldByKey案例（创建一个pairRDD，计算相同key对应值的相加结果）

[com.swenchao.spark.core.Spark14_Oper13](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark14_Oper13.scala)

16. combineByKey案例（创建一个pairRDD，根据key计算每种key的均值。（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果））

[com.swenchao.spark.core.Spark15_Oper14](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark15_Oper14.scala)

17. 以上内容综合使用案例（统计出每一个省份广告被点击次数的TOP3）

[com.swenchao.spark.core.adTop3](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/adTop3.scala)

18. JSON文件处理

[com.swenchao.spark.core.Spark18_Json](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark18_Json.scala)

19. Spark连接mysql

[com.swenchao.spark.core.Spark19_Mysql](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark19_Mysql.scala)

20. 分布式共享数据（累加器）

[com.swenchao.spark.core.Spark20_ShareData](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark20_ShareData.scala)

21. 自定义累加器（筛选带有"H"的单词）

[com.swenchao.spark.core.Spark21_Accumulator](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/core/Spark21_Accumulator.scala)

### SparkSQL

1. 读取文件内容成DF，进行展示

[com.swenchao.spark.sql.SparkSQL01_demo](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/sql/SparkSQL01_demo.scala)

2. sql方式访问数据

[com.swenchao.spark.sql.SparkSQL02_SQL](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/sql/SparkSQL02_SQL.scala)

3. RDD DF DS之间转换

[com.swenchao.spark.sql.SparkSQL03_Transform](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/sql/SparkSQL03_Transform.scala)

4. 用户自定义聚合函数

[com.swenchao.spark.sql.SparkSQL05_UDAF](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/sql/SparkSQL06_UDAF.scala)

5. 改进用户自定义聚合函数

[com.swenchao.spark.sql.SparkSQL06_UDAF_Class](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/sql/SparkSQL06_UDAF_Class.scala)

### SparkStreaming

1. 使用 SparkStreaming 完成 WordCount

[com.swenchao.spark.streaming.SparkStreaming01_WordCount](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/streaming/SparkStreaming01_WordCount.scala)

2. 从文件中获取word进行统计

[com.swenchao.spark.streaming.SparkStreaming02_FileDataSource](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/streaming/SparkStreaming02_FileDataSource.scala)

3. 自定义采集器

[com.swenchao.spark.streaming.SparkStreaming03_MyReceiver](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/streaming/SparkStreaming03_MyReceiver.scala)

4. 有状态数据统计

[com.swenchao.spark.streaming.SparkStreaming04_UpdateState.scala](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/streaming/SparkStreaming04_UpdateState.scala)

5. scala窗口操作

[com.swenchao.spark.streaming.SparkStreaming05_Window](https://github.com/Swenchao/SparkCode/blob/master/src/main/scala/com/swenchao/spark/streaming/SparkStreaming05_Window.scala)
