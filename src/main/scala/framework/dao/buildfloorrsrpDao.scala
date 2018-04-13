package framework.dao

import org.apache.spark.sql.Row

/**
  * Created by Administrator on 2017/12/29.
  */
object buildfloorrsrpDao {

  def insert2db_buildfloorrsrp(rows: Iterator[Row]): Unit = {

    val sql = "insert into BuildFloorRsrp (BuildId, Floor, Rsrp) values(?,?,?)"
    JdbcManager.executeBatchRows(rows,sql)
  }
}
