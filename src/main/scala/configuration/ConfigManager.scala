package configuration

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

/**
  * Created by Administrator on 2017/9/26.
  */
object ConfigManager {
  Logger.getLogger("org").setLevel(Level.WARN)
  private val prop=new Properties()
  private val inputStream=ConfigManager.getClass.getClassLoader.getResourceAsStream("config.properties")
  prop.load(inputStream)


  def getProperty(key: String)={
    prop.getProperty(key)
  }
  def getInteger(key:String)={
    prop.getProperty(key).toInt
  }

  def main(args: Array[String]): Unit = {
    print(getProperty("lotqci"))

  }


}



