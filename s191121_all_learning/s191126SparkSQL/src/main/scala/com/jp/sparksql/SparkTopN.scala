package com.jp.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * @author lmx
  * @born 23:58 2019-11-26
  */
object SparkTopN {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("topN").getOrCreate()
    //通过sparkSession直接读取json文件转换成为一个df
    val scoreDF: DataFrame = sparkSession.read.json("file:///F:\\scala与spark课件资料教案\\spark课程\\3、spark第三天\\资料\\score.txt")
    //查看score表当中的字段
    scoreDF.printSchema()

    scoreDF.createOrReplaceTempView("score")

    sparkSession.sql("select * from score").show()

    //需求，求每个班级成绩分数最高的top3
    /**
      *  |-- clazz: long (nullable = true)
 |-- name: string (nullable = true)
 |-- score: long (nullable = true)
      */
    // sparkSession.sql("select clazz,name,score ,rank()  over (partition by clazz order by score desc ) rankOver  from score having   rankOver <= 3").show()
    //使用子查询来找前三名 , 注意这里使用having不是where
    sparkSession.sql("select  *  from  (select clazz,name,score ,rank()  over (partition by clazz order by score desc ) rankOver  from score ) tempTable where tempTable.rankOver <= 3 ").show()

    /**
      *
      *
      * +-----+----+-----+--------+
|clazz|name|score|rankOver|
+-----+----+-----+--------+
|    1|   c|   95|       1|
|    1|   a|   80|       2|
|    1|   b|   78|       3|

|    3|   f|   99|       1|
|    3|   g|   99|       1|
|    3|   j|   78|       3|
|    3|   i|   55|       4|
|    3|   h|   45|       5|


|    2|   e|   92|       1|
|    2|   d|   74|       2|
+-----+----+-----+--------+
      *
      *
      */



    sparkSession.stop()


    /**
      *  println("rank（）跳跃排序，有两个第二名时后边跟着的是第四名\n" +
      "dense_rank() 连续排序，有两个第二名时仍然跟着第三名\n" +
      "over（）开窗函数：\n" +
      "       在使用聚合函数后，会将多行变成一行，而开窗函数是将一行变成多行；\n" +
      "       并且在使用聚合函数后，如果要显示其他的列必须将列加入到group by中，\n" +
      "       而使用开窗函数后，可以不使用group by，直接将所有信息显示出来。\n" +
      "        开窗函数适用于在每一行的最后一列添加聚合函数的结果。\n" +
      "常用开窗函数：\n" +
      "   1.为每条数据显示聚合信息.(聚合函数() over())\n" +
      "   2.为每条数据提供分组的聚合函数结果(聚合函数() over(partition by 字段) as 别名) \n" +
      "         --按照字段分组，分组后进行计算\n" +
      "   3.与排名函数一起使用(row number() over(order by 字段) as 别名)\n" +
      "常用分析函数：（最常用的应该是1.2.3 的排序）\n" +

      clazz  name  score
      //求每个班级当中成绩最大的那个值
      select  clazz,max(score)  from  score  group  by  clazz
      使用开窗函数，我们select的字段，没有限制了
      select  clazz,name,score  , max() over (partition by clazz order  score desc) from score_table

      "   1、row_number() over(partition by ... order by ...)\n" +
      "   2、rank() over(partition by ... order by ...)\n" +
      "   3、dense_rank() over(partition by ... order by ...)\n" +
      "   4、count() over(partition by ... order by ...)\n" +
      "   5、max() over(partition by ... order by ...)\n" +
      "   6、min() over(partition by ... order by ...)\n" +
      "   7、sum() over(partition by ... order by ...)\n" +
      "   8、avg() over(partition by ... order by ...)\n" +
      "   9、first_value() over(partition by ... order by ...)\n" +
      "   10、last_value() over(partition by ... order by ...)\n" +
      "   11、lag() over(partition by ... order by ...)\n" +
      "   12、lead() over(partition by ... order by ...)\n" +
      "lag 和lead 可以 获取结果集中，按一定排序所排列的当前行的上下相邻若干offset 的某个行的某个列(不用结果集的自关联）；\n" +
      "lag ，lead 分别是向前，向后；\n" +
      "lag 和lead 有三个参数，第一个参数是列名，第二个参数是偏移的offset，第三个参数是 超出记录窗口时的默认值")

      */




  }
}
