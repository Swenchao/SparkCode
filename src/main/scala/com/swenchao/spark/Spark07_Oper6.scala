package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/24 下午 08:57
 * @Func: groupBy
 */
object Spark07_Oper6 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 生成数据
        val listRDD: RDD[Int] = sc.makeRDD(1 to 4)

        // 将一个分区数据放到一个数组中（分组后形成了对偶元组（k-v），k表示分组key，v表示分组数据集合）
        // (0,CompactBuffer(2, 4))
        // (1,CompactBuffer(1, 3))
        // 可见其中Int是分组号，Iterable[Int]是组内元素
        val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

        // 打印最终结果
        groupByRDD.collect().foreach(println)
    }
}
