package com.jp.sparkstreaming

/**
  * @author lmx
  * @born 23:37 2019-11-27
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//todo:利用sparkStreaming接受socket实现一定时间内的热门词汇（出现频率比较高的词汇）
object SparkStreamingSocketWindowHotWords {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketWindowHotWords").setMaster("local[2]")

    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //3、创建streamingContext
    val ssc = new StreamingContext(sc,Seconds(5))

    //4、接受socket数据
    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.200.100",9999)

    //5、切分每一行获取所有的单词
    val words: DStream[String] = socketDstream.flatMap(_.split(" "))

    //6、每个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    //7、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(5))

    //8、按照单词出现的次数排序
    val sortDStream: DStream[(String, Int)] = result.transform(rdd => {
      //可以针对于rdd中每一个单词出现的次数降序排列
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      //取出出现次数最多的前3位
      val sortWordNumDesc: Array[(String, Int)] = sortRDD.take(3)
      //打印结果数据
      println("--------------Top3-------------start")
      sortWordNumDesc.foreach(println)
      println("--------------Top3-------------end")

      sortRDD
    })

    //打印当前批次排序后的结果数据
    sortDStream.print()


    //开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }

}
