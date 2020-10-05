package com.swenchao.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 17:38
 * @Description: RDD DF DS之间转换
 * @Modified:
 * @Version:
 */
object SparkSQL03_Transform {
    def main(args: Array[String]): Unit = {

        // 创建配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_demo")

        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 创建RDD
        val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

        // 转换为DF(在转换前需要导入隐式转换规则)
        // 这里的spark不是包名，是sparksession对象的名字
        import spark.implicits._
        val df: DataFrame = rdd.toDF("id", "name", "age")

        // 转换为DS
        val ds: Dataset[User] = df.as[User]

        // DS转换成DS
        val dsToDf: DataFrame = ds.toDF()

        // 转换为RDD
        val dfToRDD: RDD[Row] = dsToDf.rdd

        // 输出查看
        dfToRDD.foreach(row => {
            // 通过索引获取数据
            println(row.get(1))
        })

        /*** RDD直接转换成DS ***/
        // 转换成样例对象
        val userRDD: RDD[User] = rdd.map(x => {
            User(x._1, x._2, x._3)
        })

        // 样例对象转换成DS
        val rddToDS: Dataset[User] = userRDD.toDS()

        // DS转换成RDD
        val dsToRDD: RDD[User] = rddToDS.rdd

        // 输出查看
        dsToRDD.foreach(println)
        spark.stop
    }
}

// 样例类
case class User(id: Int, name: String, age: Int)
