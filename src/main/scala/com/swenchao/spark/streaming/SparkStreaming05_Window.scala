package com.swenchao.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/6 14:57
 * @Description: scala窗口操作
 * @Modified:
 * @Version:
 */
object SparkStreaming05_Window {
    def main(args: Array[String]): Unit = {

        val ints = List(1, 2, 3, 4, 5, 6)

        // 滑动窗口（第一个参数：窗口大小；第二个参数：步长）
        val intses: Iterator[List[Int]] = ints.sliding(2, 2)

        intses.foreach(println)

    }
}
