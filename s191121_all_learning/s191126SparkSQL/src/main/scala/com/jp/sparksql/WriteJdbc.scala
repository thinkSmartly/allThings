package com.jp.sparksql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/**
  * @author lmx
  * @born 23:38 2019-11-26
  *
  * 读取person.txt的文件，转换成为datafrmat
  * 然后通过datafrmat将数据写入到mysql表当中去
  */
case class Person(id:Int,name:String,age:Int)

object WriteJdbc {
  def main(args: Array[String]): Unit = {
    // val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("writeMysql").getOrCreate()

    val sparkSession: SparkSession = SparkSession.builder().appName("writeMysql").getOrCreate()


    //获取sparkContext
    val context: SparkContext = sparkSession.sparkContext

    val fileRDD: RDD[String] = context.textFile(args(0))

    //fileRDD切割
    val map: RDD[Array[String]] = fileRDD.map(x => x.split(" "))
    //将数组转换成为样例类

    val personRDD: RDD[Person] = map.map(x => Person(x(0).toInt,x(1),x(2).toInt))


    import sparkSession.implicits._
    //将样例类的RDD转换成为df
    val personDF: DataFrame = personRDD.toDF()
    personDF.show()
    //将df注册成为一张临时表
    personDF.createOrReplaceTempView("t_person")
    //查询表数据，得到result结果
    val result: DataFrame = sparkSession.sql("select * from t_person")
    val url = "jdbc:mysql://192.168.22.21:3306/userdb"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","admin")

    result.write.mode(SaveMode.Append).jdbc(url,"person",properties)

    context.stop()
    sparkSession.stop()

    //读取文本文件，转换成df，将df的数据插入到mysql表当中去

  }
}
