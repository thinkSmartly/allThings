package com.jp.sparksql

/**
  * @author lmx
  * @born 21:31 2019-11-27
  */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SmallToBigger(line:String)

object SparkUDF {
  def main(args: Array[String]): Unit = {
    //用户自定义函数
    //读取数据，转换成为DF，注册成为一张表，注册udf函数，在表查询当中使用UDF
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkUDF").master("local[2]").getOrCreate()

    val context: SparkContext = sparkSession.sparkContext

    val fileRDD: RDD[String] = context.textFile("file:///F:\\learningData\\test_debug_file\\udf.txt")
    //获取我们样例类的rdd
    val smallToBigger: RDD[SmallToBigger] = fileRDD.map(x => SmallToBigger(x))

    //导入隐式转换的包
    import sparkSession.implicits._

    val smallDF: DataFrame = smallToBigger.toDF()
    smallDF.printSchema()

    //df注册成为一张临时表
    smallDF.createOrReplaceTempView("small_table")


    //通过sparkSession来注册一个udf的函数
    sparkSession.udf.register("smallToBig",new UDF1[String,String] {
      override def call(t1: String): String = {
        //判空
        t1.toUpperCase()
      }
    },StringType)


    //使用udf的函数
    sparkSession.sql("select smallToBig(line) from small_table").show()

    context.stop()
    sparkSession.stop()


  }


}

