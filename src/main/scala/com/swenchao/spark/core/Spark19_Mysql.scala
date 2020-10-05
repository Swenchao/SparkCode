package com.swenchao.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Swenchao
 * @Date: 2020/10/04 下午 09:33
 * @Func: Mysql连接
 */
object Spark19_Mysql {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        // 定义mysql参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://127.0.0.1:3306/rdd"
        val userName = "root"
        val passWd = "123456"

        /***************询数据*********************/
        //创建 JdbcRDD，访问数据库
//        val selectRDD = new JdbcRDD(
//            sc,
//            () => {
//                // 获取数据库连接对象
//                Class.forName(driver)
//                DriverManager.getConnection(url, userName, passWd)
//            },
//            "select name, age from user where id >= ? and id <= ?",
//            // 1是sql的第一个问号，2是sql的第二个问号
//            1,
//            3,
//            2,
//            (rs) => {
//                // sql中取的第一个是name ，取的第二个是age，所以是string 1， int 2
//                println(rs.getString(1), rs.getInt(2))
//            }
//        )
//
//        //打印最后结果
//        selectRDD.collect()

        /*******保存数据********/
        val saveRDD: RDD[(String, Int)] = sc.makeRDD(List(("韩七", 20), ("周八", 23), ("吴九", 17)), 2)

        // 此部分在executor中执行，所以要新增的三条数据不一定发给了谁，不一定谁先执行，因此其中的id可能不是这个顺序
//        saveRDD.foreach({
//            case (name, age) => {
//
//                // 新增多少条数据，就会创建多少个connection，所以效率很低。但是connection又不可以序列化，所以无法把这两句提到foreach外面
//                Class.forName(driver)
//                val connection: Connection = DriverManager.getConnection(url, userName, passWd)
//
//                val sql = "insert into user (name, age) values (?, ?)"
//
//                val statement: PreparedStatement = connection.prepareStatement(sql)
//                statement.setString(1, name)
//                statement.setInt(2, age)
//                statement.executeUpdate()
//
//                statement.close()
//                connection.close()
//            }
//        })

        /*******优化的新增数据***********/
        // 以分区为整体来建立与mysql的连接（一个分区用一个connection），但是由于以分区为单位来执行，因此可能会出现内存溢出
        saveRDD.foreachPartition(datas => {
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, passWd)
            datas.foreach({
                case (name, age) => {
                    val sql = "insert into user (name, age) values (?, ?)"
                    val statement: PreparedStatement = connection.prepareStatement(sql)
                    statement.setString(1, name)
                    statement.setInt(2, age)
                    statement.executeUpdate()

                    statement.close()
                }
            })
            connection.close()
        })

        sc.stop()
    }
}