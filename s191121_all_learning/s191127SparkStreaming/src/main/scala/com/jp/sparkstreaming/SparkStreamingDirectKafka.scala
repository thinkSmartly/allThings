package com.jp.sparkstreaming

/**
  * @author lmx
  * @born 0:24 2019-11-28
  */
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

//todo:利用sparkStreaming接受kafka数据，实现单词统计------低级api（偏移量不在由zk保存）
object SparkStreamingDirectKafka {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingDirectKafka")
      .setMaster("local[4]")

    //2、创建sparkcontext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、当前构建streamingcontext，需要2个参数，一个是sparkcontext,一个是批处理时间间隔
    val ssc = new StreamingContext(sc, Seconds(5))
    //4、消息的偏移量就会被写入到checkpoint中
    ssc.checkpoint("./spark-direct")

    //4、kafka相关参数   //metadata.broker.list 老版本的kafka集群地址
    val kafkaParams = Map("metadata.broker.list" -> "node1:9092,node2:9092,node3:9092", "group.id" -> "spark-direct")
    //5、指定topic相关信息
    val topics = Set("spark")
    //6、通过KafkaUtils.createDirectStream 利用低级api接受kafka数据
    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //7、获取kafka中topic中的数据
    val data: DStream[String] = kafkaDstream.map(_._2)
    //8、切分每一行，获取所有的单词
    val words: DStream[String] = data.flatMap(_.split(" "))
    //9、每个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //10、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //11、打印结果数据
    result.print()

    //开启流式计算
    ssc.start()
    ssc.awaitTermination()


  }
}

