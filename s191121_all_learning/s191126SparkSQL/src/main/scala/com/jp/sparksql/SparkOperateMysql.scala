package com.jp.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author lmx
  * @born 23:27 2019-11-26
  */
object SparkOperateMysql {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("sparkmysql").getOrCreate()
    val url:String = "jdbc:mysql://localhost:3306/userdb"

    val properties = new Properties()
    properties.setProperty("user","root")//用户名的属性只能是user
    properties.setProperty("password","admin")

    val jdbc: DataFrame = sparkSession.read.jdbc(url,"emp",properties)
    //dsl

    //sql语法
    jdbc.show()

    sparkSession.stop()

  }

}
