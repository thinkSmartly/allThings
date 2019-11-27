package com.jp.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author lmx
  * @born 22:44 2019-11-26
  */
object StructDF {
  def main(args: Array[String]): Unit = {
    //构建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("localSqlOperate").getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val pRDD: RDD[String] = sc.textFile("F:\\learningData\\test_debug_file\\person.txt")

    val personRDD: RDD[Row] = pRDD.map(x => {
      val arr: Array[String] = x.split(" ")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val structType: StructType = (new StructType()).add("id", IntegerType, false).add("name", StringType, false)
      .add("age", IntegerType, true)

    val personDF: DataFrame = sparkSession.createDataFrame(personRDD,structType)

    personDF.createOrReplaceTempView("person_01")
    sparkSession.sql("select * from person_01 order by age").show(4)



  }

}
