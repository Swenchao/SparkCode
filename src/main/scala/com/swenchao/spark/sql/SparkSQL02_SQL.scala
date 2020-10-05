package com.swenchao.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 17:38
 * @Description: sql方式访问数据
 * @Modified:
 * @Version:
 */
object SparkSQL02_SQL {
    def main(args: Array[String]): Unit = {

        // 创建配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_demo")

        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取文件数据
        val frame: DataFrame = spark.read.json("in/user.json")

        // 将DF转换成视图
        frame.createOrReplaceTempView("user")

        // 采用sql访问
        spark.sql("select * from user").show

        spark.stop
    }
}
