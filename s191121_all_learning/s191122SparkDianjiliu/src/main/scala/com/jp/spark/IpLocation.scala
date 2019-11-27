package com.jp.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author lmx
  * @born 20:43 2019-11-25
  */
object IpLocation {
  def main(args: Array[String]): Unit = {
    //构建sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("localIpLocation")
    //第一步：获取sparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //获取ip范围布控集
    val ipRdd: RDD[String] = sc.textFile("F:\\learningData\\test_debug_file\\ip.txt")
    val ipRange: Array[(Long, Long, String, String)] = ipRdd.map(x => {
      val log: Array[String] = x.split("\\|")
      (log(2).toLong, log(3).toLong, log(13), log(14))
    }).collect()

    //通过sc调用broadcast进行广播, 将数据值存入到广播变量里面去
 val gbbl: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipRange)
    //可以在mappartition()里面调用gbbl.value来取出广播变量的值来使用
//    val value: Array[(Long, Long, String, String)] = gbbl.value
    

    //    ipRange.foreach(println(_))
    //    (3683334679,3683334679,114.879365,30.447711)
    //获取日志数据
    val logRdd: RDD[String] = sc.textFile("F:\\learningData\\test_debug_file\\20090121000132.394251.http.format")
    logRdd.map(x => {
      val ipLong: Long = ipToLong(x.split("\\|")(1))
      var i: Int = 0
      var j: Int = ipRange.length - 1
      var m: Int = 0
      var flag: Boolean = true
      //      println(ipRange(1)._1)
      while (flag) {
        m = (i + j) / 2

        if (ipRange(m)._1 > ipLong) {
          j = m
        } else if (ipRange(i)._2 < ipLong) {
          if (m == i) {
            m = j
            flag = false
          }
          i = m
        } else {
          flag = false
        }
      }

      (ipLong,ipRange(m)._2, ipRange(m)._3, ipRange(m)._4)
    }).foreach(println(_))

    sc.stop()
  }


  //todo:将IP地址装换成long  192.168.200.150  这里的.必须使用特殊切分方式[.]
  def ipToLong(ip: String): Long = {
    val ipArray: Array[String] = ip.split("[.]")
    var ipNum = 0L

    for (i <- ipArray) {
      //位或  左移8位
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }
}
