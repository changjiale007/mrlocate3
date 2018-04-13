package framework.dao

import org.apache.spark.sql.Row

/**
  * Created by Administrator on 2017/12/29.
  */
object mroIfonDao {
  def insert2db_Info(rows: Iterator[Row]): Unit = {

    val sql = "insert into  MRORecored (objectid, mmeues1apid,time_stamp,gridId,height) values(?,?,?,?,?)"
    //    select objectid,mmeues1ap,time_stamp,gridid,height from mroInfo
    JdbcManager.executeBatchRows(rows,sql)
  }
}
