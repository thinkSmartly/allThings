import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author lmx
  * @born 22:39 2019-11-21
  */
object Test001 {
  def main(args: Array[String]): Unit = {
    // 构建sparkContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("localTest001")
    val sc = new  SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //生成RDD, 并指定分区数为2.
    val arr2RDD: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")),2)

    println("first --> "+arr2RDD.partitions.size)

    // 对于(k,v)形式的RDD可以进行重新分区, 指定分区函数.
    val hsRDD: RDD[(Int, String)] = arr2RDD.partitionBy(new HashPartitioner(3))

    println("second --> "+hsRDD.partitions.size)

    println(arr2RDD.collect().toBuffer)

    //查看每个分区里面的情况 使用glom()算子.
    println(arr2RDD.glom().map(_.toBuffer).collect().toBuffer)

  }

}
