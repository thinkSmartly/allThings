package com.jp.sparkstreaming

/**
  * @author lmx
  * @born 22:49 2019-11-27
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//todo:利用sparkStreaming接受socket数据，实现单词统计
object SparkStreamingSocket {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocket").setMaster("local[2]")
    //2、创建sparkcontext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、当前构建streamingcontext，需要2个参数，一个是sparkcontext,一个是批处理时间间隔
    val ssc = new StreamingContext(sc,Seconds(5))

    //4、对接socket数据,需要socket服务的地址、端口、默认的存储级别
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.249",9998)

    //5、切分每一行
    val wordsDStream: DStream[String] = dstream.flatMap(_.split(" "))

    //6、每一个单词为1
    val wordAndOneDstream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //7、相同单词出现次数累加
    val result: DStream[(String, Int)] = wordAndOneDstream.reduceByKey(_+_)

    //8、打印输出结果
    result.print()

    //9、开启流式计算
    ssc.start()
    //一直会阻塞,等待退出.
    ssc.awaitTermination()

  }
}
