package framework.spark

import configuration.AppSettings
import model.mdt
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2018/3/6.
  */
object readCsv {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark=AppSettings.setConf()
    import spark.implicits._
  val abc=  spark.read.format("com.databricks.spark.csv").options(Map("path" -> "D:\\0-10\\TD-LTE_IMM-MDT_0801_HUAWEI_100092252097_20170719000000.csv","header" -> "true")).load()
    abc.schema.foreach(print(_))



  }
}
