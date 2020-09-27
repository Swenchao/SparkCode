package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: filter
 */
object Spark08_Oper7 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val listRDD: RDD[Int] = sc.makeRDD(1 to 4)

        // %2余数为0留下，余数不为0拿走
        val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)

        // 打印最终结果
        filterRDD.collect().foreach(println)
    }
}
