package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: sample
 */
object Spark09_Oper8 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        // 从指定数据集合中进行抽样处理，根据不同的算法进行抽样

        // 有放回
        // val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)

        // 无放回
        val sampleRDD: RDD[Int] = listRDD.sample(true, 4, 1)

        // 打印最终结果
        sampleRDD.collect().foreach(println)
    }
}
