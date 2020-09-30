package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: 自定义分区器使用
 */
object Spark12_Oper11 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        // 这种形式（默认单词类型）情况下，它的算子大部分是在RDD里面的
//        val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))

        val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

        val partitionRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))

        partitionRDD.saveAsTextFile("output")
    }
}

/**
 * 自定义分区器
 * @param partitions
 */
class MyPartitioner (partitions: Int) extends Partitioner {
    override def numPartitions: Int = {
        partitions
    }

    // 将所有数据都放到2号分区
    override def getPartition(key: Any): Int = {
        2
    }
}
