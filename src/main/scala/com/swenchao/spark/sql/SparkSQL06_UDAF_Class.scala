package com.swenchao.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 19:23
 * @Description: 用户自定义聚合函数
 * @Modified:
 * @Version:
 */
object SparkSQL06_UDAF_Class {
    def main(args: Array[String]): Unit = {

        // 创建配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_demo")

        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 创建聚合函数对象
        val udaf = new MyAgeAvgClassFunc

        // 将聚合函数装换成查询列
        val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

        val frame: DataFrame = spark.read.json("in/user.json")

        // 导入隐式转换规则
        import spark.implicits._
        val userDS: Dataset[UserBean] = frame.as[UserBean]

        // 应用函数
        userDS.select(avgCol).show()

        spark.stop
    }
}

// 因为从文件中读取，所以并没有具体范围只知道是个数字，所以要用大范围
case class UserBean (name: String, age: BigInt)
case class AvgBuffer (var sum: BigInt, var count: Int)

// 声明用户自定义聚合函数（强类型）
// 1. 继承 UserDefinedAggregateFunction
// 2. 实现方法
class MyAgeAvgClassFunc extends Aggregator[UserBean, AvgBuffer, Double] {

    // 缓冲区初始化
    override def zero: AvgBuffer = {
        AvgBuffer(0, 0)
    }

    /*
     * @Author: Swenchao
     * @Method: reduce
     * @Description: 聚合数据
     * @Param: [b, a]
     * @Return com.swenchao.spark.sql.AvgBuffer
     * @Date 2020/10/5 下午 07:59
     * @Version: 1.0
     */
    override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
        b.sum = b.sum + a.age
        b.count = 1 + b.count
        b
    }

    /*
     * @Author: Swenchao
     * @Method: merge
     * @Description: 缓冲区合并
     * @Param: [b1, b2]
     * @Return com.swenchao.spark.sql.AvgBuffer
     * @Date 2020/10/5 下午 08:00
     * @Version: 1.0
     */
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
        b1.count = b1.count + b2.count
        b1.sum = b1.sum + b2.sum
        b1
    }

    // 完成计算
    override def finish(reduction: AvgBuffer): Double = {
        reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}