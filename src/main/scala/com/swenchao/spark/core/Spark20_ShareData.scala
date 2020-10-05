package com.swenchao.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/04 下午 19:33
 * @Func: 分布式共享数据
 */
object Spark20_ShareData {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // 累加求和
//        val sum: Int = dataRDD.reduce(_ + _)
//        println(sum)

        // 简化(错误版本)：下面这样是不能实现的，因为我们的dataRDD是两个分区，但是sum现在在driver中，所以序列化分别传到两个分区中后是
        // 相互隔离的，在两个分区中分别求和后没法再两个分区相加。另外在分别求完和后的sum也没法从两个分区传回driver，所以没法实现。
//        var sum:Int = 0
//        dataRDD.foreach(i => {
//            sum = sum + i
//        })
//        println(sum)

        // 简化(正确版本)：使用累加器

        // 创建累加器对象
        val accumulator: LongAccumulator = sc.longAccumulator

        dataRDD.foreach{
            case i => {
                // 执行累加器
                accumulator.add(i)
            }
        }

        println(accumulator.value)


        sc.stop()
    }
}