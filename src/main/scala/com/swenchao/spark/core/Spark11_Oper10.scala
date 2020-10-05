package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: coalesce
 */
object Spark11_Oper10 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

        // 缩减分区数量（可以简单理解为合并分区——最后两个，并未打乱顺序）
        println("缩减分区前：" + listRDD.partitions.size)

        val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
        println("缩减分区后：" + coalesceRDD.partitions.size)

        // 保存文件
        coalesceRDD.saveAsTextFile("output")
    }
}
