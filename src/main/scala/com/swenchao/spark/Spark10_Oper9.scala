package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: distinct
 */
object Spark10_Oper9 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val listRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

//        val distinctRDD: RDD[Int] = listRDD.distinct()

        // 重组后的数据分成两个分区保存
        val distinctRDD: RDD[Int] = listRDD.distinct(2)


        // 打印最终结果
//        distinctRDD.collect().foreach(println)

        // 保存文件
        distinctRDD.saveAsTextFile("output")
    }
}
