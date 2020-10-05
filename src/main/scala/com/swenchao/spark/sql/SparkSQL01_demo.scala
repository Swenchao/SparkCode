package com.swenchao.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 下午 04:38
 * @Description: 读取文件内容成DF，进行展示
 * @Modified:
 * @Version:
 */
object SparkSQL01_demo {
    def main(args: Array[String]): Unit = {

        // 创建配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_demo")

        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取文件数据
        val frame: DataFrame = spark.read.json("in/user.json")

        // 展示
        frame.show()

        spark.stop
    }
}
