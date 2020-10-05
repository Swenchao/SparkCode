package com.swenchao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

/**
 * @Author: Swenchao
 * @Date: 2020/10/03 下午 09:33
 * @Func: Json文件处理
 */
object Spark18_Json {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 读取json文件
        val jsonRDD: RDD[String] = sc.textFile("in/user.json")

        // 进行解析
        val res: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull)

        // 输出
        res.foreach(println)

        sc.stop()
    }
}