package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: 所有元素乘以2（mapPartitions）
 */
object Spark03_Oper2 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // map算子
        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        // mapPartitions可以对一个RDD中所有的分区进行遍历，不是数据
        val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {

            // _*2是scala的东西不算是一个计算（只有交给executor的才算计算）
            // datas.map(_*2)这个整体是一个计算，要整块发给executor
            // mapPartitions效率优于map算子，因为减少了执行器网络交互
            // 虽然效率高，但是可能会出现内存溢出（因为它是按区操作，整个区操作不完，不会释放内存）
            datas.map(_*2)
        })

        // 打印最终结果
        mapPartitionsRDD.collect().foreach(println)
    }
}
