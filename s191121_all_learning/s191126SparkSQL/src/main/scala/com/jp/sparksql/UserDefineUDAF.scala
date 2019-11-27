package com.jp.sparksql

/**
  * @author lmx
  * @born 21:41 2019-11-27
  */
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class UserDefineUDAF  extends UserDefinedAggregateFunction{
  //输入的数据类型的schema
  override def inputSchema: StructType = {
    StructType(StructField("input",LongType)::Nil)
  }
  //缓冲区数据类型schema，说白了就是转换之后的数据的schema
  //10个数据   所有数据值累加 /10
  override def bufferSchema: StructType = {
    StructType(StructField("sum",LongType)::StructField("total",LongType)::Nil)
  }
  //返回值的数据类型
  override def dataType: DataType = {
    DoubleType
  }
  //确定是否相同的输入会有相同的输出
  override def deterministic: Boolean = {
    true
  }
  //初始化内部数据结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //两个初始值，一个是我们从外界传进来的数据，一个是我们累加之后一共有多少条数据
    buffer(0) = 0L
    buffer(1) = 0L
  }
  //更新数据内部结构
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //所有的金额相加
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //一共有多少条数据
    buffer(1) = buffer.getLong(1) + 1
  }
  //来自不同分区的数据进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  //计算输出数据值
  override def evaluate(buffer: Row): Any = {
    // buffer.getLong(0) 所有的金额的累加值
    // buffer.getLong(1) 一共输入多少条数据
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}



object SparkFunctionUDAF {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkUDAF").master("local[2]").getOrCreate()
    //通过sparkSession读取json文件得到DataFrame
    val employeeDF: DataFrame = sparkSession.read.json("file:///F:\\learningData\\test_debug_file\\udaf.txt")
    //通过DataFrame创建临时表
    employeeDF.createOrReplaceTempView("employee_table")
    //注册我们的自定义UDAF函数
    sparkSession.udf.register("avgSal",new UserDefineUDAF)
    //调用我们的自定义UDAF函数
    sparkSession.sql("select avgSal(salary) from employee_table").show()

    sparkSession.stop()

  }
}
