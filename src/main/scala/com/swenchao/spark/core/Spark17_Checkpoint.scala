package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/03 下午 08:25
 * @Func: 检查点（设置检查点就是将血缘关系保存成文件）
 */
object Spark17_Checkpoint {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 设定检查点保存目录
        sc.setCheckpointDir("CheckPoint")

        // 构造rdd
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 进行简单处理
        val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
        val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

        // 设置检查点
        reduceRDD.checkpoint()

        // 输出查看
        reduceRDD.foreach(println)

        // 血缘关系
        println(reduceRDD.toDebugString)

        sc.stop()
    }
}