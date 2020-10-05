package com.swenchao.spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/04 下午 19:59
 * @Func: 自定义累加器
 */
object Spark21_Accumulator {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop", "Hive", "HBase", "Scala", "Spark"), 2)

        // TODO 创建累加器
        val wordAccumulator = new WordAccumulator()
        // TODO 注册累加器
        sc.register(wordAccumulator)

        dataRDD.foreach{
            case word => {
                // 执行累加器
                wordAccumulator.add(word)
            }
        }

        // TODO 获取累加器值
        println(wordAccumulator.value)


        sc.stop()
    }
}

/**
 * 声明累加器
 * 1. 继承 AccumulatorV2
 * 2. 实现抽象方法
 * 3. 创建累加器
 */
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]]  {

    val list = new util.ArrayList[String]()

    // 当前累加器是否为初始化状态
    override def isZero: Boolean = list.isEmpty

    // 复制累加器
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
        new WordAccumulator()
    }

    // 重置累加器
    override def reset(): Unit = {
        list.clear()
    }

    // 向累加器中增加数据
    override def add(v: String): Unit = {
        if (v.contains("H")) {
            list.add(v)
        }
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
        list.addAll(other.value)
    }

    // 获取累加器结果
    override def value: util.ArrayList[String] = list
}