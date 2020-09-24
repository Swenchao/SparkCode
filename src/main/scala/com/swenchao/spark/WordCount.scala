package com.swenchao.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/9/22 下午 10:14
 * @Func: WordCount
 */
object WordCount {
    def main(args: Array[String]): Unit = {

        // local模式

        //创建conf对象
        // 设定计算框架运行（部署）环境
        // app id对应一个应用名称
        val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建上下文对象
        val sc = new SparkContext(config)

        // 读取文件,将文件内容按行读取
        // 路径查找位置默认从当前部署环境中查找
        // 如需从本地查找 file:///opt/module/spark/in
        val lines: RDD[String] = sc.textFile("in/word.txt")

        // 分解成单词
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // 将单词数据进行结构转换
        val wordToOne: RDD[(String, Int)] = words.map((_, 1))

        // 分组聚合
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

        // 打印结果
        val res = wordToSum.collect()
//        println(res)
        res.foreach(println)
    }

}
