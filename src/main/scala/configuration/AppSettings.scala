package configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

object AppSettings {
  val maxHeight = 200
  val deltaHeight = 5


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
        .master("local")
      .appName("Mrlocate"+System.currentTimeMillis()).enableHiveSupport().getOrCreate()

//    hiveContext.sql("set hive.exec.compress.output=true")
//    hiveContext.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.BzipCodec")
//    hiveContext.sql("set mapred.output.compression.type=BLOCK")
    hiveContext.sql("set hive.exec.compress.intermediate=true")
    hiveContext.sql("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
    hiveContext.sql("set hive.intermediate.compression.type=BLOCK")

    hiveContext.sql("set hive.exec.max.dynamic.partitions=10000")
    hiveContext.sql("set hive.exec.max.dynamic.partitions.pernode=10000")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveContext.sql("set datanucleus.fixedDatastore=ture")
    hiveContext.sql("set datanucleus.autoCreateSchema=false")

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
        .master("local")
      .appName("Mrlocate"+System.currentTimeMillis()).enableHiveSupport().getOrCreate()
//    hiveContext.sql("set hive.exec.compress.output=true")
//    hiveContext.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.BzipCodec")
//    hiveContext.sql("set mapred.output.compression.type=BLOCK")
    hiveContext.sql("set hive.exec.compress.intermediate=true")
    hiveContext.sql("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
    hiveContext.sql("set hive.intermediate.compression.type=BLOCK")

    hiveContext.sql("set hive.exec.max.dynamic.partitions=10000")
    hiveContext.sql("set hive.exec.max.dynamic.partitions.pernode=10000")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveContext.sql("set datanucleus.fixedDatastore=ture")
    hiveContext.sql("set datanucleus.autoCreateSchema=false")
    hiveContext

  }
}
