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
 * @Date: 2020/10/6 14:25
 * @Description: 有状态数据统计
 * @Modified:
 * @Version:
 */
object SparkStreaming04_UpdateState {
    def main(args: Array[String]): Unit = {

        // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.foldLeft(0)(_ + _)
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }

        val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, Seconds(3))

        // 保存数据状态需要设置检查点路径
        ssc.checkpoint("/streamCheck")

        // Create a DStream that will connect to hostname:port, like hadoop102:9999
        val lines = ssc.socketTextStream("hadoop102", 9999)

        // Split each line into words
        val words = lines.flatMap(_.split(" "))

        //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
        // Count each word in each batch
        val pairs = words.map(word => (word, 1))


        // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
        val stateDstream = pairs.updateStateByKey[Int](updateFunc)
        stateDstream.print()

        //val wordCounts = pairs.reduceByKey(_ + _)

        // Print the first ten elements of each RDD generated in this DStream to the console
        //wordCounts.print()

        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
        //ssc.stop()
    }
}
