package com.jp.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @author lmx
  * @born 23:09 2019-11-26
  * 这是使用hivesql语法,并不是真正的与hive整合.
  * 以后碰到结构化的数据,直接加载到hive里面来,再这样进行分析就很简单了
  */
object SparkHiveSql {
  def main(args: Array[String]): Unit = {
    //使用enableHiveSupport 开启与hive的整合，让spakr的sql语句直接兼容hive的语法
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("sparkhive").enableHiveSupport().getOrCreate()
    //创建hive表
    sparkSession.sql("create table if not exists student2 (id int ,name string,age int ) row format delimited fields terminated by ','")
    //向hive表当中加载数据
    sparkSession.sql("load data local inpath './data/student.csv' overwrite into table student2")
    sparkSession.sql("select * from student2").show()
    sparkSession.stop()
  }

}
