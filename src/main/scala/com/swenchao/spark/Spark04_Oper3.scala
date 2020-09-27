package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: mapPartitionsWithIndex
 */
object Spark04_Oper3 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // map算子
        val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

        val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
            case (num, datas) => {
                datas.map((_, "，分区号：" + num))
            }
        }

        // 打印最终结果
        tupleRDD.collect().foreach(println)
    }
}
