package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/22 下午 10:14
 * @Func: 统计出每一个省份广告被点击次数的TOP3
 */
object adTop3 {
    def main(args: Array[String]): Unit = {

        //创建conf对象
        // app id对应一个应用名称
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdTop3")

        // 创建上下文对象
        val sc = new SparkContext(config)

        // 读取文件生成RDD
        val lines: RDD[String] = sc.textFile("in/agent.log")

        // 分解成：((Province,AD),1)
        // 原来样式：时间戳 省份 城市 用户 广告
        val provinceAD: RDD[((String, String), Int)] = lines.map(x => {
            val details: Array[String] = x.split(" ")
            ((details(1), details(4)), 1)
        })
        // 检验样式：((5,10),1)
//        provinceAD.foreach(println)

        // 点击次数相加
        val sumProvinceAD: RDD[((String, String), Int)] = provinceAD.reduceByKey((x, y) => x + y)
//        sumProvinceAD.foreach(println)

        // 改变样式 (Province,(AD,1))
        val provinceToADSum: RDD[(String, (String, Int))] = sumProvinceAD.map(x => {
            (x._1._1, (x._1._2, x._2))
        })
//        provinceToADSum.foreach(x=>println(x._1))

        // 根据省份分组
        val provinceSum: RDD[(String, Iterable[(String, Int)])] = provinceToADSum.groupByKey()
//        provinceSum.foreach(println)

        // 排序取前三
        val provinceADTop3: RDD[(String, List[(String, Int)])] = provinceSum.mapValues(x => {
            x.toList.sortWith((x, y) => x._2 < y._2).take(3)
        })

        provinceADTop3.foreach(println)
//        provinceADTop3.saveAsTextFile("output")

        //9.关闭与spark的连接
        sc.stop()
    }
}
