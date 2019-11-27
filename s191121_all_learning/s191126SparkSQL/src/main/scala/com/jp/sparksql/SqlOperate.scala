package com.jp.sparksql

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author lmx
  * @born 21:54 2019-11-26
  */
case class Person(id:Int,name:String,age:Int)

object SqlOperate {
  def main(args: Array[String]): Unit = {
    //构建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("localSqlOperate").getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val pRdd: RDD[String] = sc.textFile("F:\\learningData\\test_debug_file\\person.txt")

    val personRdd: RDD[Person] = pRdd.map(x => {
      val arr: Array[String] = x.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    //将样例类的RDD转化成为df
    //如果要将我们的rdd转化成为df，一定要导入样例类
    import  sparkSession.implicits._
    //通过样例类的RDD调用toDF方法，转化成为一个df
    val personDF: DataFrame = personRdd.toDF

    println("======sql style=======")
    personDF.registerTempTable("person01") //方法过期, 推荐使用下面那个方法
    sparkSession.sql("select name from person01").show()
    personDF.createOrReplaceTempView("person02")
    sparkSession.sql("select name from person02").show()

    println("======dsl style=======")
    personDF.select($"name").filter($"age" > 18).show()
    personDF.groupBy("age").count().show()

    sc.stop()
    sparkSession.stop()

  }

}
