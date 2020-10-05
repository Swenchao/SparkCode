package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/29 下午 08:57
 * @Func: foldByKey案例
 */
object Spark14_Oper13 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val foldRDD: RDD[(Int, Int)] = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

        // 查看分区
//        val glomRDD: RDD[Array[(String, Int)]] = aggRDD.glom()
//
//        glomRDD.collect().foreach(s
//            => {println(s.mkString(","))}
//        )

        // 相加(foldByKey案例)
        val resRDD: RDD[(Int, Int)] = foldRDD.foldByKey(0)(_ + _)

        // combineByKey求和
//        val resRDD: RDD[(Int, Int)] = foldRDD.combineByKey(x=>x,(x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)

        resRDD.collect().foreach(println)
    }
}
