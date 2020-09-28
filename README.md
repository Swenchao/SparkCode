# SparkCode

学习Spark所写demo实践

博客会定时更新Spark学习笔记——https://swenchao.github.io/ 

小白学习笔记，多包涵~

## 文件说明

com.swenchao.spark.WordCount    依然是给出的 WordCount 样例

com.swenchao.spark.Spark01_RDD  生成RDD方式（从集合中创建RDD）

com.swenchao.spark.Spark02_Oper1    map案列（创建一个1-10数组的RDD，将所有元素*2形成新的RDD）

com.swenchao.spark.Spark03_Oper2    mapPartitions案列（创建一个1-10数组的RDD，将所有元素*2形成新的RDD）

com.swenchao.spark.Spark04_Oper3    mapPartitionsWithIndex案例（创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD）

com.swenchao.spark.Spark05_Oper4    flatMap案例（创建一个元素为1-4的RDD，运用flatMap创建一个新的RDD，将所有数字分开）

com.swenchao.spark.Spark06_Oper5    glom案例（创建一个4个分区的RDD，并将每个分区的数据放到一个数组）

com.swenchao.spark.Spark07_Oper6    groupBy案例（创建一个RDD，按照元素模以2的值进行分组）

com.swenchao.spark.Spark08_Oper7    filter案例（创建一个RDD（1 2 3 4），过滤出一个新RDD（%2为0的））

com.swenchao.spark.Spark09_Oper8    sample(withReplacement, fraction, seed) 案例（创建一个RDD（1-10），从中选择放回和不放回抽样）

com.swenchao.spark.Spark10_Oper9    distinct案例（创建一个RDD，使用distinct()对其去重）

com.swenchao.spark.Spark11_Oper10    coalesce案例（创建一个4个分区的RDD，对其缩减分区）



