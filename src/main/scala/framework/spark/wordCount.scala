package framework.spark

import java.util

import configuration.AppSettings
import org.apache.commons.lang.ArrayUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/1/9.
  */
object wordCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark=SparkSession.builder().master("local[2]").appName("aa").getOrCreate()
    spark.sql("select 'b' = '1'").show()
//    val rdd1=spark.sparkContext.parallelize(
//      Array(
//        ("1-1","a"),("1-1","c"),("1-2","a"),
//        ("1-2","b"),("1-2","d"),("1-3","b"),("1-3","e"),
//        ("1-3","f"),("1-1","b")
//      )
//    )
//   val rdd2=rdd1.map(kv=>(kv._2,kv._1))
//    val rdd3=rdd2.groupByKey()
//    val rdd4= rdd3.map(kv=>{
//      (kv._2.min,1)
//    })
//
//
//   rdd4.reduceByKey(_+_).sortBy(_._1).collect().foreach(print(_))
  }
}
