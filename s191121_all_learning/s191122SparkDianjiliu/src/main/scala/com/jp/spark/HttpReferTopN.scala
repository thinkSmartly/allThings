package com.jp.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author lmx
  * @born 22:41 2019-11-22
  */
object HttpReferTopN {
  def main(args: Array[String]): Unit = {
    //构建sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("localPVCount")
    //第一步：获取sparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val logRdd: RDD[String] = sc.textFile("F:\\learningData\\test_debug_file\\access.log")

    val records: Array[String] = logRdd.map(_.split(" ")).filter(_.size > 10).map(x => (x(10), 1))
      .reduceByKey(_ + _).sortBy(x => x._2, false).map(x => x._2 + x._1)
      .take(10)
    println(records.toBuffer)

    sc.stop()
  }

}
