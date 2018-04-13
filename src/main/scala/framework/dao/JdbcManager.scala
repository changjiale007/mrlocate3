package framework.dao

import java.sql.{Connection, PreparedStatement, SQLException}

import configuration.DBConnection
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Administrator on 2017/12/29.
  */
object JdbcManager {

  var key = ""
  val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  var ip = "10.254.188.160"
  var databasename = "iNek_GuiYang"//iNek_shanghai_test
  var username = "sa"
  var password = "new.1234"

  def getUrl() = {
    "jdbc:sqlserver://"+ip+":1433;DatabaseName="+databasename+";user="+username+";password="+password
  }

  def getConnection(): Connection = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    java.sql.DriverManager.getConnection(getUrl(), username, password)
  }

  /**
    * 获取一个表的dataframe
    * @param hc
    * @param tableName
    * @return
    */
  def Table(hc:HiveContext, tableName:String) : DataFrame = {
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    hc.read.format("jdbc").options(Map("url" -> DBConnection.getUrl(), "driver" -> driver, "dbtable" -> tableName)).load()
  }

  /**
    * 定义增删改方法
    */
  def executeBatch(sql: String, paramList: Array[Array[Any]]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = getConnection()

      for (params <- paramList) {
        pstmt = connection.prepareStatement(sql)
        for (i <- 0 until (params.length)) {

          pstmt.setObject(i + 1, params(i))
        }
        pstmt.executeUpdate()
      }


    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        if (pstmt != null) pstmt.close()
        if (connection != null) connection.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }

  }
  def executeBatchRows(rows: Iterator[Row],sql:String): Unit = {
    val conn = DBConnection.getConnection()
    var ps: java.sql.PreparedStatement = null
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(sql)
    var count=0
    try {
      for(row <-rows){
        for(i <-0 until(row.length)){
          ps.setObject((i+1),row.get(i))
        }
        ps.addBatch()
        count+=1
        if(count==500){
          ps.executeBatch()
          conn.commit()
          count=0
          ps.close()
          ps = conn.prepareStatement(sql)
        }
      }


    }
    catch {
      case e: Exception => throw e
    } finally {
      conn.commit()
      if (ps != null)
        ps.close()
      if (conn != null) {
        conn.close()
      }
    }
  }

}
