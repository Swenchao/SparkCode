package com.swenchao.spark.sql

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author: Swenchao
 * @Date: 2020/10/5 19:23
 * @Description: 用户自定义聚合函数
 * @Modified:
 * @Version:
 */
object SparkSQL05_UDAF {
    def main(args: Array[String]): Unit = {

        // 创建配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_demo")

        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 自定义聚合函数
        // 创建聚合函数对象
        val udaf = new MyAgeAvgFunc
        spark.udf.register("avgAge", udaf)

        // 使用聚合函数
        val frame: DataFrame = spark.read.json("in/user.json")
        frame.createOrReplaceTempView("user")

        spark.sql("select avgAge(age) from user").show

        spark.stop
    }
}

// 声明用户自定义聚合函数
// 1. 继承 UserDefinedAggregateFunction
// 2. 实现方法
class MyAgeAvgFunc extends UserDefinedAggregateFunction {

    // 函数输入结构
    override def inputSchema: StructType = {
        new StructType().add("age", LongType)

    }

    // 计算时数据结构
    override def bufferSchema: StructType = {
        new StructType().add("sum", LongType).add("count", LongType)
    }

    // 函数返回类型
    override def dataType: DataType = DoubleType

    // 函数是否稳定（传入相同值，最终结果是否一样）
    override def deterministic: Boolean = true

    // 当前函数计算之前缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 因为没有属性值的意义，所以buffer相当于是一个数组，其中buffer(0)是第一个值，相当于sum；buffer(1)为count
        // 都初始化为0
        buffer(0) = 0L
        buffer(1) = 0L
    }

    // 根据查询结果更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//        buffer(0) = buffer.getLong(0) + input.getLong(0)
//        buffer(1) = buffer.getLong(1) + 1
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
    }

    // 将多个节点缓冲区合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // sum
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        // count
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算
    override def evaluate(buffer: Row): Any = {
        buffer.getLong(0).toDouble / buffer.getLong(1)
    }
}

