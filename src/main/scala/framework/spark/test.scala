package framework.spark


import java.util.Properties

import configuration.AppSettings
import org.apache.commons.lang3.ArrayUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

/**
  * Created by Administrator on 2018/5/24.
  */
object test {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.WARN)
    val spark=AppSettings.setConf()
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "root")
//    //如果找不到mysql的的驱动类并抛异常，可以加入以下代码，不抛异常下面代码可以不用
//    // connectionProperties.put("driver","com.mysql.jdbc.Driver")
//    spark.read.jdbc("jdbc:mysql://localhost:3306/kettle?useUnicode=true&characterEncoding=UTF-8", "rand", connectionProperties).createOrReplaceTempView("aa")
//    spark.sql(
//      """
//        |
//        |select if(id1 is null ,0,1) as meizhi,
//        |       if(id1 is not null,1,0) as youzhi
//        |       from aa
//      """.stripMargin).show(20)
    import spark.implicits._
    spark.createDataset(Array(1,2,3,4)).show(10)

    val a=Array(1,2,3)
    val b=Array(4,5,6)
   print( a.exists(b.contains(_)))
  }
}
