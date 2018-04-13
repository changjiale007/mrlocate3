package framework.dao

import org.apache.spark.sql.Row

/**
  * Created by Administrator on 2018/1/15.
  */
object finger0TestDao {
  def insert2db_ftest(rows: Iterator[Row]): Unit = {

    val sql = "insert into ftest2 (GridID, Longitude, Latitude,RSRP) values(?,?,?,?)"
    JdbcManager.executeBatchRows(rows,sql)

  }
}
