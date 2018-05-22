package configuration

import java.sql.Connection

/**
 * Created by xuximing on 2016/5/19.
 */
object DBConnection {
  var key = ""
  val driver = ConfigManager.getProperty("driver")
  var ip = ConfigManager.getProperty("ip")
  var databasename = ConfigManager.getProperty("databasename")//newguiyang"//iNek_shanghai_test
  var username = ConfigManager.getProperty("username")
  var password = ConfigManager.getProperty("password")

  def getUrl() = {
    "jdbc:sqlserver://"+ip+":1433;DatabaseName="+databasename+";user="+username+";password="+password
  }

    def getConnection(): Connection = {
    Class.forName(driver)
    java.sql.DriverManager.getConnection(getUrl(), username, password)
  }

}
