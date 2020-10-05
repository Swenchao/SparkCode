package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/29 下午 08:57
 * @Func: combineByKey案例
 */
object Spark15_Oper14 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val combineRDD: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

        // 相加
        val sumRDD: RDD[(String, (Int, Int))] = combineRDD.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        sumRDD.collect().foreach(println)

        // 求平均值
        val resRDD: RDD[(String, Double)] = sumRDD.map { case (key, value) => (key, value._1 / value._2.toDouble) }
        resRDD.collect().foreach(println)
    }
}
