package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: 生成RDD第一种方式（从集合中创建RDD）
 */
object Spark01_RDD {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 创建RDD
        // 从内存中创建 makeRDD(其底层实现，其实就是调用了parallelize)
//        val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 从内存中创建 parallelize
//        val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

        // 从外部存储中创建（默认情况可读取项目路径。也可以读取其他路径）
        val value: RDD[String] = sc.textFile("in")

//        listRDD.collect().foreach(println)
        value.saveAsTextFile("output")
    }
}
