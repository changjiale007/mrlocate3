package configuration

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by xuximing on 2017/3/10.
 */
object DBHelper {
//  def Table(hc:SparkSession, url:String, tableName:String) : DataFrame = {
//    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
//    hc.load("jdbc", Map("url" -> url, "driver" -> driver, "dbtable" -> tableName))
//
//  }

  def Table(hc:SparkSession, tableName:String) : DataFrame = {
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    hc.read.format("jdbc").options(Map("url" -> DBConnection.getUrl(), "driver" -> driver, "dbtable" -> tableName)).load()
  }
}
