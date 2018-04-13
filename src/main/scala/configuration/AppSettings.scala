package configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by xuximing on 2016/4/18.
 */
object AppSettings {
  val maxHeight = 200
  val deltaHeight = 5

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    val spark=AppSettings.setConf(conf)
    spark.sql("select * from class_test").show(10)
  }

  def setConf(): SparkSession ={
    val conf=new SparkConf()
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.shuffle.memoryFraction", "0.6")
      .set("spark.port.maxRetries", "500")
      .set("spark.driver.maxResultSize","20g")
    val hiveContext = SparkSession.builder()
      .config(conf)
      .appName("Mrlocate"+System.currentTimeMillis()).enableHiveSupport().getOrCreate()
    hiveContext

  }
  def setConf(conf:SparkConf): SparkSession ={
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.shuffle.memoryFraction", "0.6")
      .set("spark.port.maxRetries", "500")
      .set("spark.driver.maxResultSize","20g")
    val hiveContext = SparkSession.builder()
      .config(conf)
      .appName("Mrlocate"+System.currentTimeMillis()).enableHiveSupport().getOrCreate()
    hiveContext

  }
}
