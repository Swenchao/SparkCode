package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: flatMap
 */
object Spark05_Oper4 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // map算子
        val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

        // flatMap拆成 1 2 3 4
        val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

        // 打印最终结果
        flatMapRDD.collect().foreach(println)
    }
}
