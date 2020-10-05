package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/29 下午 08:57
 * @Func: RDD中的函数传递（序列化）
 */
object Spark16_Serializable {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "bigData"))

        // 创建search对象
        val search = new Search("h")

        // 运用第一个过滤函数并打印结果
//        val match1: RDD[String] = search.getMatch1(rdd)
//        match1.collect().foreach(println)

        // 运用第二个
        val match2: RDD[String] = search.getMatche2(rdd)
        match2.collect().foreach(println)

        sc.stop()
    }
}

class Search(query: String) extends java.io.Serializable {

    //过滤出包含字符串的数据
    def isMatch(s: String): Boolean = {
        s.contains(query)
    }

    //过滤出包含字符串的RDD
    // 此方法是要在executor中执行，而此方法是一个成员方法（来源于某个对象），因此在使用的时候，这个类也要传给executor（因此这个类也需要序列化）
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
        rdd.filter(isMatch)
    }

    //过滤出包含字符串的RDD
    def getMatche2(rdd: RDD[String]): RDD[String] = {
        // 虽然都是filter，但是此处试一个匿名方法，不再是类方法。另外query为对象属性，仍然需要传递类到executor的
        rdd.filter(x => x.contains(query))
    }

}
