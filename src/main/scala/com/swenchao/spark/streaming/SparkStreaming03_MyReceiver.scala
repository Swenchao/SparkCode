package com.swenchao.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/6 下午 12:30
 * @Description: 自定义采集器
 * @Modified:
 * @Version:
 */
object SparkStreaming03_MyReceiver {
    def main(args: Array[String]): Unit = {

        // 1.初始化Spark配置信息
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
            .setAppName("StreamWordCount")

        // 2.初始化SparkStreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        // 3.从指定文件夹中采集数据
        val receiverDStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))

        // 4.将每一行数据做切分，形成一个个单词
        val wordStreams = receiverDStream.flatMap(_.split(" "))

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

// 声明采集器
// 1. 继承Receiver
// 2. 实现方法（onStart、onStop）
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    var socket: Socket = _

    def receive(): Unit = {
        socket = new Socket(host, port)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

        var line: String = null

        while ((line = reader.readLine()) != null) {

            // 将采集的数据存储到采集器的内部进行转换
            if ( "END".equals(line) ) {
                return
            } else {
                this.store(line)
            }
        }
    }

    override def onStart(): Unit = {
        new Thread(new Runnable {
            override def run(): Unit = {
                receive()
            }
        }).start()
    }

    override def onStop(): Unit = {
        if (socket != null) {
            socket.close()
            socket = null
        }
    }
}
