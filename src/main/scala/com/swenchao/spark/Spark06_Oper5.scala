package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: glom
 */
object Spark06_Oper5 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)
        val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 5)

        // 将一个分区数据放到一个数组中
        val glomRDD: RDD[Array[Int]] = listRDD.glom()
        

        // 打印最终结果
        glomRDD.collect().foreach(array => {
            println(array.mkString(","))
        })
    }
}
