/**
  * @author lmx
  * @born 20:57 2019-11-25
  */
object IpLocationTest001 {
  def main(args: Array[String]): Unit = {
    val ip = "1.0.63.255"
    val l: Long = ipToLong(ip)
    println(l)

  }

  //todo:将IP地址装换成long   192.168.200.150  这里的.必须使用特殊切分方式[.]
  def ipToLong(ip: String): Long = {
    val ipArray: Array[String] = ip.split("[.]")
    var ipNum=0L

    for(i <- ipArray){
      ipNum=i.toLong | ipNum << 8L
    }
    ipNum
  }

}
