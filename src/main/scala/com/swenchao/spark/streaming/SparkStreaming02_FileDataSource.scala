package com.swenchao.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 下午 09:30
 * @Description: 从文件中获取word进行统计
 * @Modified:
 * @Version:
 */
object SparkStreaming02_FileDataSource {
    def main(args: Array[String]): Unit = {

        // 1.初始化Spark配置信息
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
            .setAppName("StreamWordCount")

        // 2.初始化SparkStreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // 3.从指定文件夹中采集数据
        val dirStream = ssc.textFileStream("test")

        // 4.将每一行数据做切分，形成一个个单词
        val wordStreams = dirStream.flatMap(_.split(" "))

        // 5.将单词映射成元组（word,1）
        val wordAndOneStreams = wordStreams.map((_, 1))

        // 6.将相同的单词次数做统计
        val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

        // 7.打印
        wordAndCountStreams.print()

        // 8.启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
