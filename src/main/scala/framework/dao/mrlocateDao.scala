package framework.dao

import java.sql.Date
import java.text.SimpleDateFormat

import configuration.DBConnection
import org.apache.spark.sql.Row

/**
  * Created by Administrator on 2017/12/29.
  */
object mrlocateDao {
  def insert2db_mrlocate(rows: Iterator[Row], day:String): Unit = {
    val sql = "insert into MRLocate_1222 (GridID, ObjectID,  MRPointNum, OverlapRatioCount, " +
      " AverageLteScRSRP, AverageRSRQ, AverageLteScsinrUL, rsrp_140_110_count, rsrp_110_100_count, rsrp_100_90_count, rsrp_90_80_count, rsrp_80_count, " +
      " sinrul_3_count, sinrul_3_0_count, sinrul_0_10_count, sinrul_10_15_count, sinrul_15_count, ReportTime, height,overlap) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val rows2=rows.toList
    val params=new Array[Array[Any]](rows2.size)
    for (i<- 0 until(rows2.size)){
      params(i)=Array(
          rows2(i).getLong(0),
          rows2(i).getLong(1).toInt,
          rows2(i).getLong(2).toInt,
          rows2(i).getLong(3).toInt,
          rows2(i).getDouble(4),
          rows2(i).getDouble(5),
          rows2(i).getDouble(6),
          rows2(i).getInt(7),
          rows2(i).getInt(8),
          rows2(i).getInt(9),
          rows2(i).getInt(10),
          rows2(i).getInt(11),
          rows2(i).getInt(12),
          rows2(i).getInt(13),
          rows2(i).getInt(14),
          rows2(i).getInt(15),
          rows2(i).getInt(16),
      new Date(dateFormat.parse(day).getTime),
          rows2(i).getInt(17),
          rows2(i).getInt(18).toDouble
      )
    }

  }
  def insert2db_mrlocate(rows: Iterator[Row], day:String,flag:Boolean): Unit = {
    val conn = DBConnection.getConnection()
    var ps: java.sql.PreparedStatement = null
    val sql = "insert into MRLocate_1333 (GridID, objectid, MRPointNum, OverlapRatioCount, " +
      " AverageLteScRSRP, AverageRSRQ, AverageLteScsinrUL, rsrp_140_110_count, rsrp_110_100_count, rsrp_100_90_count, rsrp_90_80_count, rsrp_80_count, " +
      " sinrul_3_count, sinrul_3_0_count, sinrul_0_10_count, sinrul_10_15_count, sinrul_15_count, ReportTime, height,overlap) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
//    val sql="insert into MRLocate_0115_9 (GridID,longitude,latitude,RSRP) values (?,?,?,?)"
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    try {
      rows.foreach(row => {
        ps = conn.prepareStatement(sql)
        /**
          * hiveContext.sql("create table if not exists mrlocate" +
//      " (gridid bigint, objectid bigint ,mrpointnum bigint, weakpointnum bigint, avgrsrp double, avgrsrq double, avgsinrul double, " +
//      " rsrp_140_110_count int, rsrp_110_100_count int, rsrp_100_90_count int, rsrp_90_80_count int, rsrp_80_count int, " +
//      " sinrul_3_count int, sinrul_3_0_count int, sinrul_0_10_count int, sinrul_10_15_count int, sinrul_15_count int, height int,overlapd int)
          */
        ps.setLong(1, row.getLong(0))
        ps.setInt(2, row.getLong(1).toInt)
        //        ps.setDouble(3, row.getDouble(2))
        //        ps.setDouble(4, row.getDouble(3))
        ps.setInt(3, row.getLong(2).toInt)
        ps.setInt(4, row.getLong(3).toInt)
        ps.setDouble(5, row.getDouble(4))
        ps.setDouble(6, row.getDouble(5))
        ps.setDouble(7, row.getDouble(6))
        ps.setInt(8, row.getInt(7))
        ps.setInt(9, row.getInt(8))
        ps.setInt(10, row.getInt(9))
        ps.setInt(11, row.getInt(10))
        ps.setInt(12, row.getInt(11))
        ps.setInt(13, row.getInt(12))
        ps.setInt(14, row.getInt(13))
        ps.setInt(15, row.getInt(14))
        ps.setInt(16, row.getInt(15))
        ps.setInt(17, row.getInt(16))
        ps.setDate(18, new Date(dateFormat.parse(day).getTime))
        ps.setInt(19, row.getInt(17))
        ps.setDouble(20,row.getInt(18))
//        ps.setString(1,row.getString(0))
//        ps.setDouble(2,row.getDouble(1))
//        ps.setDouble(3,row.getDouble(2))
//        ps.setDouble(4,row.getDouble(3))
        ps.executeUpdate()
        ps.close()
      })

    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null) {
        conn.close()
      }
    }
  }



  def insert2db(rows: Iterator[Row]): Unit = {
    val conn = DBConnection.getConnection()
    var ps: java.sql.PreparedStatement = null

    val sql = "insert into QCI (gridid,ULDLQCICount,ulqci1count,ulqci1rate,dlqci1count,dlqci1rate,pusum,pdsum) values(?,?,?,?,?,?,?,?)"
    //gridid bigint,udtotal bigint,uptotal int,dltotal int,uplost double,dllost double,pusum int, pdsum int
    //gridid,udtotal,uptotal,uplost,dltotal,dllost,pusum,pdsum
    try {
      rows.foreach(row => {
        ps = conn.prepareStatement(sql)
        //gridid
        ps.setLong(1, row.getLong(0))
        //ULDLQCICount
        ps.setInt(2, row.getLong(1).toInt)
        //ulqci1count
        ps.setInt(3, row.getInt(2))
        //ulqci1rate
        ps.setDouble(4, row.getDouble(3))
        //dlqci1count
        ps.setInt(5, row.getInt(4))
        //dlqci1rate
        ps.setDouble(6,row.getDouble(5))
        ps.setInt(7,row.getInt(6))
        ps.setInt(8,row.getInt(7))
        ps.executeUpdate()
        ps.close()
      })
    }
    catch {
      case e: Exception => throw e
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null) {
        conn.close()
      }
    }
  }
}
