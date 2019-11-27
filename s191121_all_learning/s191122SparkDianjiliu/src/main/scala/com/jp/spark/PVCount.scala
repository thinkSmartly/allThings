package com.jp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lmx
  * @born 21:57 2019-11-22
  */
object PVCount {
  def main(args: Array[String]): Unit = {
    //构建sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("localPVCount")
    //第一步：获取sparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val logRdd: RDD[String] = sc.textFile("F:\\learningData\\test_debug_file\\access.log")

    logRdd.map(x => {
      val record: Array[String] = x.split(" ")
      (record(6),1)
    }).reduceByKey(_+_)
      .saveAsTextFile("F:\\learningData\\test_debug_file\\result_access.log")
    //      .foreach(println(_))

    sc.stop()
  }

}
