package framework.dao

import configuration.DBConnection
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Administrator on 2017/12/29.
  */
object qciDao {
  def insert2db(rows: Iterator[Row]): Unit = {


    val sql = "insert into QCI1 (gridid,ULDLQCICount,ulqci1count,ulqci1rate,dlqci1count,dlqci1rate,pusum,pdsum) values(?,?,?,?,?,?,?,?)"
    JdbcManager.executeBatchRows(rows,sql)
  }

  def insert2db(df:DataFrame): Unit ={
    df.write.mode("overwrite").options(Map("url" -> DBConnection.getUrl(), "driver" -> DBConnection.driver, "dbtable" -> "QCI1"))
  }
}
