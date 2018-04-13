package framework.spark

import configuration.{AppSettings, ConfigManager}
import framework.dao.JdbcManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/1/16.
  */
object threed_mrlocate_one {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val logger=Logger.getLogger(threed_mrlocateV3.getClass)
    val time = args(0)
    val partitionNum = args(1).toInt
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    val conf = new SparkConf().setAppName("threed_mrlocate")
    AppSettings.setConf(conf)

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //动态的获取表名
    val fingerOneTableName = ConfigManager.getProperty("fingerOnePartition")
    val fingerHighTableName = ConfigManager.getProperty("fingerHighPartition")

    val mroPartitionTableName =  ConfigManager.getProperty("mroPartitionTable")
    val mroFloor = ConfigManager.getProperty("mroFloorTable")
    val mroResult = ConfigManager.getProperty("mroResultTable")
    val database=ConfigManager.getProperty("dataBaseName")
    //结果合并小文件
    hiveContext.sql("set hive.merge.mapredfiles = true;")
    hiveContext.sql("set hive.merge.mapredfiles = true;")
    hiveContext.sql("set hive.merge.size.per.task = true;")


    mroJoinFloor(hiveContext, mroFloor, mroPartitionTableName, fingerOneTableName, fingerHighTableName, day, hour, partitionNum,database)

  }

  def mroJoinFloor(hiveContext: HiveContext, MroFloor: String, mroTableName: String, fingerOneTableName: String, fingerHighTableName: String, day: String, hour: String, partitionNum: Int,dataBase:String) = {
    hiveContext.sql("use " + dataBase + "")

    hiveContext.sql("create table if not exists " + MroFloor + " (objectid bigint, mmeues1apid string," +
      "time_stamp string,ncellobjectid bigint, ltescrsrp int,ltencrsrp int,gridid bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile")
//    //判断分区是否为第一个分区，如果是就相当于重新计算了该分区
//
//    hiveContext.sql(" insert into table " + MroFloor + " " +
//      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,c1.ncellobjectid,c1.ltescrsrp,ltencrsrp,c2.GridID  from  " +
//      "             (select * from " + mroTableName + " where day = '" + day + "' and hour = '" + hour + "')  c1 inner join " +
//      "             (select GridID,ObjectID,RSRP,n_objectid,n_rsrp,height  from " + fingerOneTableName + " where gridid=24417412 or gridid=24423031) c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid)" +
//      "         ")
//    hiveContext.sql(
//      """
//        | select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.gridid,a1.height,a1.distance from
//        | (select * from testmrofloor where day='20171222' and hour='00') a1
//        | inner join mrofloor_one a2 on a1.objectid=a2.objectid and a1.mmeues1apid=a2.mmeues1apid and a1.time_stamp=a2.time_stamp
//        |
//      """.stripMargin).foreachPartition(p=>insert2DB(p))




  }
  def  insert2DB(rows: Iterator[Row]): Unit ={
    val sql = "insert into doublemrofloor (objectid,mmeues1apid,time_stamp,ltescrsrp,gridid,height,distance ) values(?,?,?,?,?,?,?)"
    JdbcManager.executeBatchRows(rows,sql)
  }
}
