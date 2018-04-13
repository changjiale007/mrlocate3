package framework.spark

import configuration.{AppSettings, DBConnection}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/12/5.
  */
object lotqci1 {
  def main(args: Array[String]): Unit = {
    val time = args(0)
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    val hiveContext= AppSettings.setConf()
    hiveContext.sql(
      " set hive.merge.mapfiles = true;"
    )
    hiveContext.sql("use guiyang2")
//    //将结果与原始mrojoin 计算出qci 和 LteScPUSCHPRBNum 值
//    hiveContext.sql("create table if not exists result (gridid bigint,objectid bigint,time_stamp string,mmeues1apid string,ulqci1 string,dlqci1 string,ltescpuschprbnum int,ltescpdschprbnum int) partitioned by (day string)")
//
//    hiveContext.sql("insert overwrite table result partition(day="+day+") " +
//      "         select gridid,objectid,time_stamp,mmeues1apid,ulqci1,dlqci1,ltescpuschprbnum,ltescpdschprbnum from " +
//      "             (select * from doublemroresults where day="+day+") a1 inner join " +
//      "              (select * from mropartition where day="+day+")  a2 on a1.objectid=a2.objectid and a1.time_stamp=a2.time_stamp and a1.mmeues1apid=a2.mmeues1apid")
//
//    hiveContext.sql("create table lotqci (gridid bigint,uptotal int,dltotal int,uplost double,dllost double,pusum int, pdsum int)")
//    hiveContext.sql("insert into table lotqci " +
//      "     select gridid,sum(case when ulqci1=-999 then 0 else 1 end)+ sum(case when dlqci1=-999 then 0 else 1 end)-sum(case when ulqci1=-999 or dlqci1=-999 then 0 else 1 end), as udtotal" +
//      "                   sum(case when ulqci1=-999 then 0 else 1 end) as uptotal," +
//      "                    sum(case when dlqci1=-999 then 0 else 1 end) as dltotal," +
//      "                     cast(sum(case when ulqci1=-999 then 0 else ulqci1 end)/sum(case when ulqci1=-999  then 0 else 1 end) as decimal(18,4)) as uplost," +
//      "                      cast(sum(case when dlqci1=-999 then 0 else dlqci1 end)/sum(case when dlqci1=-999 then 0 else 1 end) as decimal(18,4)) as dllost," +
//      "                      sum(case when LteScPUSCHPRBNum=-999 then 0 else LteScPUSCHPRBNum end) as pusum," +
//      "                      sum(case when LteScPDSCHPRBNum=-999 then 0 else LteScPDSCHPRBNum end) as pdsum " +
//      " from result where day="+day+" group by gridid ").repartition(1)

      /*临时*/
    hiveContext.sql("select gridid,longitude,latitude,objectid,rsrp,n_objectid,n_rsrp from finger02 where longitude > 106.673430376923" +
      "  and longitude < 106.703430376923 and latitude> 26.5015301623413 and latitude < 26.5210608246687   ").foreachPartition(p=>(insertFinger(p)))

    /*************************/

//    hiveContext.sql("create table mroInfo(objectid int,mmeues1apid string,time_stamp string,ltescrsrp int,ltescrsrq int,ltescsinrul int,gridid string,longitude double,latitude double,hour string,height int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//      hiveContext.sql(" insert into table mroInfo " +
//        " select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,a1.ltescsinrul,a1.gridid,a2.longitude,a2.latitude,a1.hour,a1.height from " +
//        "   doublemroresults a1  inner join (select distinct gridid,longitude,latitude from finger02) a2 on a1.gridid=a2.gridid" +
//        "     DISTRIBUTE BY hour").repartition(1)
 //   hiveContext.sql("select objectid,mmeues1apid,time_stamp,gridid,height from mroInfo").foreachPartition(p=>insert2db_Info(p))
//        hiveContext.sql("use guiyang2")
//        hiveContext.sql(
//          """
//            | select a1.gridid,a2.longitude,a2.latitude ,a1.avgrsrp-141 from
//            |(select gridid,avg(ltescrsrp) as avgrsrp from doublemroresult where day='20171222' and hour='02' group by gridid) a1 inner join
//            | (select distinct gridid,longitude,latitude from finger02) a2 on (a1.gridid=a2.gridid)
//          """.stripMargin). foreachPartition(p=>finger0TestDao.insert2db_ftest(p))
//          hiveContext.sql(
//            s"""
//              | create table rdd as
//              | select distinct a1.gridid,a1.longitude,a1.latitude,a1.rsrp from
//              | (select distinct gridid,longitude,latitude,rsrp from finger0) a1 left join
//              | (select distinct gridid from doublemroresult where day = '20171222' and hour='00' and height = 0) a2 on a1.gridid=a2.gridid where a2.gridid is null
//              |  Distribute by 1
//            """.stripMargin)

//          hiveContext.sql("select * from rdd"). foreachPartition(p=>finger0TestDao.insert2db_ftest(p))


  }

//  def insert2db(rows: Iterator[Row]): Unit = {
//    val conn = DBConnection.getConnection()
//    var ps: java.sql.PreparedStatement = null
//    val sql = "insert into QCI (gridid, ulqci1count,ulqci1rate,dlqci1count,dlqci1rate,pusum,pdsum) values(?,?,?,?,?,?,?)"
//    //gridid bigint,uptotal int,dltotal int,uplost double,dllost double
//
//    try {
//      rows.foreach(row => {
//        ps = conn.prepareStatement(sql)
//        //buildid
//        ps.setLong(1, row.getLong(0))
//        //height
//        ps.setInt(2, row.getInt(1))
//        //rsrp
//        ps.setDouble(3, row.getDouble(3))
//        ps.setInt(4, row.getInt(2))
//        ps.setDouble(5, row.getDouble(4))
//        ps.setInt(6,row.getInt(5))
//        ps.setInt(7,row.getInt(6))
//        ps.executeUpdate()
//        ps.close()
//      })
//    }
//    catch {
//      case e: Exception => throw e
//    } finally {
//      if (ps != null)
//        ps.close()
//      if (conn != null) {
//        conn.close()
//      }
//    }
//  }
//  def insert2db_Info(rows: Iterator[Row]): Unit = {
//    val conn = DBConnection.getConnection()
//    var ps: java.sql.PreparedStatement = null
//    val sql = "insert into  MRORecored (objectid, mmeues1apid,time_stamp,gridId,height) values(?,?,?,?,?)"
//    //    select objectid,mmeues1ap,time_stamp,gridid,height from mroInfo
//    try {
//      rows.foreach(row => {
//        ps = conn.prepareStatement(sql)
//        //buildid
//        ps.setInt(1, row.getInt(0))
//        //height
//        ps.setString(2, row.getString(1))
//        //rsrp
//        ps.setString(3, row.getString(2))
//        ps.setLong(4, row.getLong(3))
//        ps.setInt(5, row.getInt(4))
//        ps.executeUpdate()
//        ps.close()
//      })
//    }
//    catch {
//      case e: Exception => throw e
//    } finally {
//      if (ps != null)
//        ps.close()
//      if (conn != null) {
//        conn.close()
//      }
//    }
//  }
//

  def insertFinger(rows: Iterator[Row])={
      val sql="insert into finger0 (gridid,longitude,latitude,objectid,rsrp,n_objectid,n_rsrp) values (?,?,?,?,?,?,?)"
        val conn = DBConnection.getConnection()
        var ps: java.sql.PreparedStatement = null
    //gridid,longitude,latitude,objectid,rsrp,n_objectid,n_rsrp
    try {
            rows.foreach(row => {
              ps = conn.prepareStatement(sql)
              //buildid
              ps.setObject(1, row.get(0))
              //height
              ps.setObject(2, row.get(1))
              //rsrp
              ps.setObject(3, row.get(2))
              ps.setObject(4,row.get(3))
              ps.setObject(5, row.get(4))
              ps.setObject(6, row.get(5))
              ps.setObject(7,row.get(6))
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