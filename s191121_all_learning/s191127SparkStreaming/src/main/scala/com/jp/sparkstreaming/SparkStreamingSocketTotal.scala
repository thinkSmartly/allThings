package com.jp.sparkstreaming

/**
  * @author lmx
  * @born 23:05 2019-11-27
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//todo:利用sparkStreaming接受socket数据，实现所有批次单词统计结果累加
object SparkStreamingSocketTotal {

  //currentValue:表示当前批次中每一个单词出现的所有的1，（hadoop,List(1,1,1,1,1)）
  //historyValue:表示之前批次中每个单词出现的次数 (hadoop,10)
  def updateFunc(currentValue:Seq[Int],historyValue:Option[Int]):Option[Int] ={
    val newValue: Int = currentValue.sum + historyValue.getOrElse(0)
    Some(newValue)
  }

  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketTotal").setMaster("local[2]")
    //2、创建sparkcontext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、当前构建streamingcontext，需要2个参数，一个是sparkcontext,一个是批处理时间间隔
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置checkpoint目录  它会保存之前批次中单词出现的次数
    ssc.checkpoint("./socket-checkpoint")

    //4、对接socket数据,需要socket服务的地址、端口、默认的存储级别
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.249",9998)

    //5、切分每一行
    val wordsDStream: DStream[String] = dstream.flatMap(_.split(" "))

    //6、每一个单词为1
    val wordAndOneDstream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //7、相同单词出现次数累加 ,需要更新某个key状态，这个状态key出现的次数
    val result: DStream[(String, Int)] = wordAndOneDstream.updateStateByKey(updateFunc)

    //8、打印输出结果
    result.print()

    //9、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}

