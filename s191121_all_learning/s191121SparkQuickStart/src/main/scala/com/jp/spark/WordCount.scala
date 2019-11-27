package com.jp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用scala的api来开发
  *
  * @author lmx
  * @born 1:02 2019-10-25
  */
object WordCount {

  def main(args: Array[String]): Unit = {
//    构建sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("localModeCount")

//    //打包到集群上面去运行
//    val sparkConf: SparkConf = new SparkConf().setAppName("localModeCount")

    //第一步：获取sparkContext对象
    val sc = new  SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //读取文件
    val file: RDD[String] = sc.textFile("file:///F:\\learningData\\test_debug_file\\wordcount.txt")
//    val file: RDD[String] = sc.textFile("hdfs://jp-bigdata-01:8020/test/wordcount.txt")

    //打包到集群上面去运行，输入的路径，动态的传入
//    val file: RDD[String] = sc.textFile(args(0))
    //调用flatMap方法，对文件内容进行切分再压平
    val map: RDD[String] = file.flatMap(_.split(" "))
    // 将每个单词记做1
    val wordAndOne: RDD[(String, Int)] = map.map((_,1))
    //统计单词一共出现了多少次
    val key: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //按照单词出现的次数进行排序
    val by: RDD[(String, Int)] = key.sortBy(x => x._2,false)
    //将我们的结果保存到文件里面去
    //给定一个文件夹的路径，将我们的结果，保存到文件里面去，路径不能存在，存在就报错
    //  by.saveAsTextFile("file:///F:\\scala与spark课件资料教案\\spark课程\\1、spark第一天\\wordcount\\outcount1")
    //输出路径也是动态的传入
//    by.saveAsTextFile(args(1))
    //打印单词出现的次数
    val collect: Array[(String, Int)] = by.collect()

    println(collect.toBuffer)
    //停止sparkContext的程序
    sc.stop()

  }
}
