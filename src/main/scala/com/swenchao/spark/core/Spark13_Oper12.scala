package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/29 下午 08:57
 * @Func: aggregateByKey案例
 */
object Spark13_Oper12 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val aggRDD: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

        // 查看分区
//        val glomRDD: RDD[Array[(String, Int)]] = aggRDD.glom()
//
//        glomRDD.collect().foreach(s
//            => {println(s.mkString(","))}
//        )

        // 取出每个分区相同key对应值的最大值，然后相加
        val resRDD: RDD[(String, Int)] = aggRDD.aggregateByKey(0)(math.max(_, _), _ + _)
        resRDD.collect().foreach(println)
    }
}
