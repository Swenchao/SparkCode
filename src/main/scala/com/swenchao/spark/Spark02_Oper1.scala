package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: 所有元素乘以2
 */
object Spark02_Oper1 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // map算子
        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        // _*2就是一个计算，要在一个executor上进行，执行了10次
        val mapRDD: RDD[Int] = listRDD.map(_*2)

        // 打印最终结果
        mapRDD.collect().foreach(println)
    }
}
