package com.swenchao.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 下午 09:30
 * @Description: 使用 SparkStreaming 完成 WordCount
 * @Modified:
 * @Version:
 */
object SparkStreaming01_WordCount {
    def main(args: Array[String]): Unit = {

        // Spark配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

        // 实时数据分析环境对象（第二个参数是采集周期：以指定时间为周期来采集数据）
        val streamingContext = new StreamingContext(sparkConf, Seconds(3))

        // 从指定端口中采集数据
        val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

        //将每一行数据做切分，形成一个个单词
        val wordStreams = lineStreams.flatMap(line => {
            line.split(" ")
        })

        //将单词映射成元组（word,1）
        val wordAndOneStreams = wordStreams.map((_, 1))

        //将相同的单词次数做统计
        val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

        //打印
        wordAndCountStreams.print()

        // 此时因为是一直采集数据，所以不能停止
//        streamingContext.stop

        //启动采集器
        streamingContext.start()
        // driver等待采集器执行
        streamingContext.awaitTermination()
    }
}
