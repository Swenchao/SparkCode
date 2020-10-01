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

        // ((5,10),1)
        val sumProvinceAD: RDD[((String, String), Int)] = provinceAD.reduceByKey((x, y) => x + y)

//        sumProvinceAD.foreach(println)

        // 改变样式

    }

}
