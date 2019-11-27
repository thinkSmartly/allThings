package com.jp.sparkstreaming

/**
  * @author lmx
  * @born 0:00 2019-11-28
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.immutable

//todo；利用sparkStreaming接受kafka数据，实现单词统计 ---利用高级Api(就是消息的偏移量保存在zk上)
object SparkStreamingReceiverKafka {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingReceiverKafka")
      .setMaster("local[4]")
      //开启WAL预写日志，保存数据源端安全性 ,开启WAL就需要在后面设置check-point目录了
      .set("spark.streaming.receiver.writeAheadLog.enable","true")
    //2、创建sparkcontext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、当前构建streamingcontext，需要2个参数，一个是sparkcontext,一个是批处理时间间隔
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置checkpoint目录，保存接收的源端数据
    ssc.checkpoint("./spark-receiver")

    //4、zk地址
    val zkQuorum="node1:2181,node2:2181,node3:2181"
    //5、消费者组id
    val groupId="spark-receiver"
    //6、指定topic相关信息   //这里map的key就是topic的名称，后面的value表示：当前一个receiver接收器采用几个线程去消费
    val topics=Map("spark" -> 1)
    //7、通过KafkaUtils.createStream 通过高级api方式将kafka跟sparkStreaming整合
    //(String, String)   第一个String表示当前topic的名称，第二个String表示消息的具体内容
    val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics)

//    val numRecevier=3
//    //采用3个receiver接收器接受数据, 如果3个进程去拉取数据, 那么local[4]里面就至少是4了, 第4个进程来计算.
//    val totalReceiver: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to numRecevier).map(x => {
//      val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
//      kafkaDstream
//    })
//
//    //通过ssc.union 把所有receiver接收器的数据整合
//    val unionkafkaDstream: DStream[(String, String)] = ssc.union(totalReceiver)

    kafkaDstream.foreachRDD(rdd=>{
      rdd.foreach(x=>println("key: "+x._1))
    })

    //8、获取kafka中topic中的数据
    val data: DStream[String] = kafkaDstream.map(_._2)
    //9、切分每一行，获取所有的单词
    val words: DStream[String] = data.flatMap(_.split(" "))
    //10、每个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //11、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //12、打印结果数据
    result.print()
    //开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }
}
