package framework.spark

import configuration.{AppSettings, ConfigManager, DBHelper}
import framework.dao.{buildfloorrsrpDao, mrlocateDao}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xuximing on 2017/6/14.....
 */
object threed_mrlocate_gridavg {
Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]) {
    val day = args(0)

    val hiveContext= AppSettings.setConf()


    //数据库名
    val databasename=ConfigManager.getProperty("databasename")
    //栅格化定位结果表
    val mroResultTable=ConfigManager.getProperty("mroResultTable")
    //合并栅格化定位后的结果表
    val mroResultTables=ConfigManager.getProperty("mroResultTables")
    //计算结果表
    val mrLocate=ConfigManager.getProperty("MRLocate")
    //重叠覆盖度表名
    val overlaps=ConfigManager.getProperty("overlaps")
    //获取0层指纹库
    val finger0=ConfigManager.getProperty("fingerOneTableName")
    //获取mro表
    val mroTable=ConfigManager.getProperty("mroTable")
    //丢包计算表
    val lotqci=ConfigManager.getProperty("lotqci")
    //输出hive最终结果表
    val resultMr=ConfigManager.getProperty("resultMr")
    //result表
    val result=ConfigManager.getProperty("result")
    hiveContext.sql(s"use ${databasename}")


    //合并小文件
    mergeSmallfile(hiveContext,day,databasename,mroResultTable,mroResultTables)
    //计算重叠覆盖数
    generateOverlap(hiveContext,day,databasename,overlaps,mroResultTables,mroTable)
    //计算栅格级和楼层级的结果
    generateMrLocate(hiveContext,day,databasename,mroResultTables,mrLocate,overlaps,finger0)
    //计算丢包率
    generateLotQCI1(hiveContext,day,databasename,result,mroResultTables,mroTable,lotqci)
    //生成最后结果
    generateResult(hiveContext,databasename,resultMr,mrLocate,lotqci,finger0)
  }

  /** 生成栅格级和楼层级的结果
    *
    * @param hiveContext hivecontext对象
    * @param day 时间
    * @param dataBaseName 数据库名
    */
  def generateMrLocate(hiveContext: SparkSession, day: String, dataBaseName: String, mroResultTables:String, MRLocate:String, overlaps:String, finger0:String) = {
    hiveContext.sql(s"use ${dataBaseName}")
    hiveContext.sql(
      s"""
        | create table if not exists ${MRLocate}
        |  (gridid string, objectid bigint ,mrpointnum bigint, weakpointnum bigint, avgrsrp double, avgrsrq double, avgsinrul double,
        |  rsrp_140_110_count int, rsrp_110_100_count int, rsrp_100_90_count int, rsrp_90_80_count int, rsrp_80_count int,
        |  sinrul_3_count int, sinrul_3_0_count int, sinrul_0_10_count int, sinrul_10_15_count int, sinrul_15_count int, height int,overlapd int )
        |  partitioned by (day string)
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      """.stripMargin)

    hiveContext.sql(
      s"""
        |insert into ${MRLocate} PARTITION (day = '${day}')
        | select m.GridID, m.ObjectID , count(1) as mrpointnum, sum(case when m.ltescrsrp-141 <= -110 then 1 else 0 end),
        | AVG(m.ltescrsrp-141) as avgrsrp, sum(case when m.ltescrsrq = -999 then 0 else m.ltescrsrq/2 -20 end)/sum(case when m.ltescrsrq = -999 then 0 else 1 end) as avgrsrq,
        | sum(case when m.ltescsinrul = -999 then 0 else m.ltescsinrul-11 end)/sum(case when m.ltescsinrul = -999 then 0 else 1 end) as avgsinrul,
        | sum(case when m.ltescrsrp-141 < -110 and m.ltescrsrp-141 >= -140 then 1 else 0 end),
        | sum(case when m.ltescrsrp-141 < -100 and m.ltescrsrp-141 >= -110 then 1 else 0 end),
        | sum(case when m.ltescrsrp-141 < -90 and m.ltescrsrp-141 >= -100 then 1 else 0 end),
        | sum(case when m.ltescrsrp-141 < -80 and m.ltescrsrp-141 >= -90 then 1 else 0 end),
        | sum(case when m.ltescrsrp-141 >= -80 then 1 else 0 end),
        | sum(case when m.ltescsinrul-11 < -3 and m.ltescsinrul > -999 then 1 else 0 end),
        | sum(case when m.ltescsinrul-11 < 0 and m.ltescsinrul-11 >= -3 then 1 else 0 end),
        | sum(case when m.ltescsinrul-11 < 10 and m.ltescsinrul-11 >= 0 then 1 else 0 end),
        | sum(case when m.ltescsinrul-11 < 15 and m.ltescsinrul-11 >= 10 then 1 else 0 end),
        | sum(case when m.ltescsinrul-11 >= 15 then 1 else 0 end),
        | m.height,case when d.overlapd is null then 0 else d.overlapd end
        |             from
        |                 (select * from ${mroResultTables} where day = '${day}') m
        |                 left join
        |                 ${overlaps} d
        |                 on m.gridid=d.gridid
        | group by m.GridID, m.Objectid,m.height,d.overlapd
        |
      """.stripMargin)


    hiveContext.sql(
      s"""
        | insert into ${MRLocate} PARTITION (day = '${day}')
        |   select distinct f.gridid, f.objectid,0, 0, f.RSRP, -999, -999, 0,0,0,0,0,0,0,0,0,0,0,0 from ${finger0} f
        |              left join
        |              (select gridid from mrlocate where day='${day}' and height=0 ) m
        |              on on f.gridid = m.gridid
        |   where m.gridid is null
      """.stripMargin)
    hiveContext.sql(s"use ${dataBaseName}")

    //入库 //判断是否入库
    val flag=false
  if(flag){
    hiveContext.sql(s"select * from ${MRLocate} where day = '${day}'").foreachPartition(p=> mrlocateDao.insert2db_mrlocate(p,day,true))
   // 计算楼层级结果
    DBHelper.Table(hiveContext, "gridmappingbuilding").createOrReplaceTempView("gridmappingbuilding_db")
    hiveContext.sql(
      s"""
        | select g.BulidingID, m.height, avg(avgrsrp) from
        |         ${MRLocate}  m
        |       inner join
        |         gridmappingbuilding_db g on m.gridid = g.gridid  where m.day = '${day}'
        |
        | group by g.BulidingID, m.height
      """.stripMargin).foreachPartition(p => buildfloorrsrpDao.insert2db_buildfloorrsrp(p))
  }
  }


  /**
    * 将小文件合并 提高计算效率
    * @param hiveContext
    * @param day 操作的那一天的数据
    */

  def mergeSmallfile(hiveContext: SparkSession, day: String,database:String,mroResultTable:String,mroResultTables:String) = {
    hiveContext.sql(s"use ${database}")
    //gridid string,objectid bigint,time_stamp string,mmeues1apid string,ulqci1 string,dlqci1 string,ltescpuschprbnum int,ltescpdschprbnum int
    hiveContext.sql(
      s"""
        |
        | create table if not exists ${mroResultTables}
        |   (objectid bigint, mmeues1apid string,time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int,gridid string, height int,hour string)
        |  partitioned by (day string)
      """.stripMargin)
    hiveContext.sql(
      s"""
        |insert into table ${mroResultTables}  partition(day = ${day})
        |       select objectid , mmeues1apid ,time_stamp , ltescrsrp , ltescrsrq , ltescsinrul ,gridid , height,hour
        |             from ${mroResultTable}
        |        where day = ${day}  DISTRIBUTE BY hour
      """.stripMargin)



  }
 //计算覆盖度
  def generateOverlap(hiveContext:SparkSession,day:String,database:String,overlaps:String,mroResultTables:String,mroTable:String): Unit ={
    hiveContext.sql(s"use ${database}")

    hiveContext.sql(s"create table if not exists ${overlaps} (gridid string,overlapd int) partitioned by (day string)")

    hiveContext.sql(
      s"""
        | insert into table ${overlaps} partition(day = '${day}')
        |     select gridid,sum(case when t.objectid is null then 0 else 1 end) as overlapd
        |         from
        |             (select * from ${mroResultTables} where day = '${day}') as d
        |             left join
        |             (select a1.objectid,a1.mmeues1apid,a1.time_stamp from
        |                      (select objectid,mmeues1apid,time_stamp,ltescrsrp,ltencrsrp from
        |                        ${mroTable} where day = '${day}'  and abs(ltescrsrp-ltencrsrp)<=6) a1
        |                             inner join
        |                         (select * from ${mroResultTables} where day = '${day}') as a2
        |                          on a1.objectid=a2.objectid and a1.mmeues1apid=a2.mmeues1apid and a1.time_stamp=a2.time_stamp
        |             group by a1.objectid,a1.mmeues1apid,a1.time_stamp having count(1)>=3
        |              ) as t
        |              on t.objectid=d.objectid and t.mmeues1apid=d.mmeues1apid and t.time_stamp=d.time_stamp
        |      group by d.gridid
      """.stripMargin)
  }

  /** 计算栅格级的丢包率
    *
    * @param hiveContext hive 对象
    * @param day 处理的天数
    */

  def generateLotQCI1(hiveContext: SparkSession, day: String,databaseName:String,result:String,mroResultTables:String,mroTable:String,lotqci:String) = {
    hiveContext.sql(s"use ${databaseName}")
////    //将结果与原始mrojoin 计算出qci 和 LteScPUSCHPRBNum 值
 hiveContext.sql(s"create table if not exists ${result} (gridid string,objectid bigint,time_stamp string,mmeues1apid string,ulqci1 string,dlqci1 string,ltescpuschprbnum int,ltescpdschprbnum int) partitioned by (day string)")
    hiveContext.sql(
      s"""
        | insert overwrite table ${result} partition(day = '${day}')
        |     select a1.gridid,a1.objectid,a1.time_stamp,a1.mmeues1apid,a2.ulqci1,a2.dlqci1,a2.ltescpuschprbnum,a2.ltescpdschprbnum
        |               from
        |                     (select * from doublemroresults where day = '${day}') a1
        |                     inner join
        |                     (select distinct objectid,time_stamp,mmeues1apid,ulqci1,dlqci1,ltescpuschprbnum,ltescpdschprbnum from ${mroTable} where day = '${day}')  a2
        |                     on a1.objectid=a2.objectid and a1.time_stamp=a2.time_stamp and a1.mmeues1apid=a2.mmeues1apid
        |
        |
       """.stripMargin)


    hiveContext.sql(s"create table if not exists ${lotqci} (gridid string,udtotal bigint,uptotal int,dltotal int,uplost double,dllost double,pusum int, pdsum int)")
    hiveContext.sql(
      s"""
        | insert overwrite table ${lotqci}
        |      select gridid,sum(case when ulqci1<0 or ulqci1 is null then 0 else 1 end)+ sum(case when dlqci1<0 or dlqci1 is null then 0 else 1 end)-sum(case when ulqci1<0 or ulqci1 is null  or dlqci1<0  or dlqci1 is null then 0 else 1 end) as udtotal,
        |             sum(case when ulqci1<0 or ulqci1 is null then 0 else 1 end) as uptotal,
        |             sum(case when dlqci1<0 or dlqci1 is null then 0 else 1 end) as dltotal,
        |             cast(sum(case when ulqci1<0 or ulqci1 is null  then 0 else ulqci1 end)/sum(case when ulqci1<0  or ulqci1 is null then 0 else 1 end) as decimal(18,4)) as uplost,
        |             cast(sum(case when dlqci1<0 or dlqci1 is null then 0 else dlqci1 end)/sum(case when dlqci1<0  or dlqci1 is null  then 0 else 1 end) as decimal(18,4)) as dllost,
        |             sum(case when LteScPUSCHPRBNum<0 or LteScPUSCHPRBNum is null then 0 else LteScPUSCHPRBNum end) as pusum,
        |             sum(case when LteScPDSCHPRBNum<0 or LteScPDSCHPRBNum is null then 0 else LteScPDSCHPRBNum end) as pdsum
        |      from ${result} where day = '${day}' group by gridid
        |
      """.stripMargin)


  }
  def generateResult(hiveContext: SparkSession,database:String,resultMr:String,mrLocate:String,lotqci:String,finger0:String) = {
    hiveContext.sql(s"use ${database}")
    hiveContext.sql(
      s"""
        | create table ${resultMr} as
        | select mr.GridID,f.Longitude,f.Latitude,
        | mr.objectid,mr.height,mr.MRPointNum,avgrsrp,
        | RSRP_140_110_COUNT,RSRP_110_100_COUNT,RSRP_100_90_COUNT,
        | RSRP_90_80_COUNT, RSRP_80_COUNT,
        | t.udtotal,
        | t.uptotal,t.uplost/10 as uprate,t.ulqci1,t.ulqci20,t.ulqci80,
        | t.dllost,t.dllost/10 as dlrate,t.dlqci1,t.dlqci20,t.dlqci80,
        | avgsinrul, pusum,pdsum,overlapd
        |  from ${mrLocate} mr
        |  left join (
        |  select m.GridID, uptotal,uplost ,pusum,pdsum,udtotal,
        |  SUM( case when uplost /10 <= 1 then MRPointNum else 0 end ) ulqci1,
        |  SUM( case when  (uplost /10 < 20 and  uplost /10 >= 1 ) then MRPointNum else 0 end) ulqci20,
        |  SUM( case when (uplost /10 < 80 and  uplost /10 >= 20 ) then MRPointNum else 0 end) ulqci80,
        |  dllost,dllost,
        |  SUM( case when dllost /10 <= 1   then MRPointNum else 0 end ) dlqci1,
        |  SUM( case when ( dllost /10 <= 20 and  dllost /10 >= 1) then MRPointNum else 0 end) dlqci20,
        |   SUM( case when dllost /10 <= 80 and  dllost /10 >= 20 then MRPointNum else 0 end) as dlqci80
        |  from ${mrLocate} m
        | left join ${lotqci} on lotqci.gridid = m.GridID
        |  group by m.GridID,uptotal,uplost,dllost,dllost,pusum,pdsum,udtotal
        | ) t on  t.gridid = mr.GridID
        | left join (select distinct gridid,longitude,latitude from  ${finger0} ) as f on f.gridid = mr.GridID
      """.stripMargin)
    //生成栅格化结果表
        hiveContext.sql("create table mroInfo(objectid int,mmeues1apid string,time_stamp string,ltescrsrp int,ltescrsrq int,ltescsinrul int,gridid string,longitude double,latitude double,hour string,height int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
        hiveContext.sql(
          s"""
            |  insert into table mroInfo
            |  select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,
            |  a1.ltescsinrul,a1.gridid,a2.longitude,a2.latitude,a1.hour,a1.height from
            |  doublemroresults a1
            |  inner join
            |  (select distinct gridid,longitude,latitude from finger02) a2
            |  on a1.gridid=a2.gridid
            |  DISTRIBUTE BY hour
          """.stripMargin)

  }














  def backUp(): Unit ={

//    hiveContext.sql("insert into mrlocate PARTITION (day = '"+day+ "')" +
//      " select m.GridID, m.ObjectID , count(1) as mrpointnum, sum(case when m.ltescrsrp-141 <= -110 then 1 else 0 end), " +
//      " AVG(m.ltescrsrp-141) as avgrsrp, sum(case when m.ltescrsrq = -999 then 0 else m.ltescrsrq/2-20 end)/sum(case when m.ltescrsrq = -999 then 0 else 1 end) as avgrsrq, " +
//      " sum(case when m.ltescsinrul = -999 then 0 else m.ltescsinrul-11 end)/sum(case when m.ltescsinrul = -999 then 0 else 1 end) as avgsinrul," +
//      " sum(case when m.ltescrsrp-141 < -110 and m.ltescrsrp-141 >= -140 then 1 else 0 end), " +
//      " sum(case when m.ltescrsrp-141 < -100 and m.ltescrsrp-141 >= -110 then 1 else 0 end), " +
//      " sum(case when m.ltescrsrp-141 < -90 and m.ltescrsrp-141 >= -100 then 1 else 0 end), " +
//      " sum(case when m.ltescrsrp-141 < -80 and m.ltescrsrp-141 >= -90 then 1 else 0 end), " +
//      " sum(case when m.ltescrsrp-141 >= -80 then 1 else 0 end), " +
//      " sum(case when m.ltescsinrul-11 < -3 and m.ltescsinrul > -999 then 1 else 0 end), " +
//      " sum(case when m.ltescsinrul-11 < 0 and m.ltescsinrul-11 >= -3 then 1 else 0 end), " +
//      " sum(case when m.ltescsinrul-11 < 10 and m.ltescsinrul-11 >= 0 then 1 else 0 end), " +
//      " sum(case when m.ltescsinrul-11 < 15 and m.ltescsinrul-11 >= 10 then 1 else 0 end), " +
//      " sum(case when m.ltescsinrul-11 >= 15 then 1 else 0 end), m.height,case when d.overlapd is null then 0 else d.overlapd end " +
//      " from (select * from  where day = '" + day + "') m  left join overlaps d on m.gridid=d.gridid" +
//      "  group by m.GridID, m.Objectid,m.height,d.overlapd ")

    ////
//    hiveContext.sql("insert into mrlocate PARTITION (day = '"+day+ "')" +
//      " select distinct f.gridid, f.objectid,0, 0, f.RSRP, -999, -999, 0,0,0,0,0,0,0,0,0,0,0,0 from finger02 f" +
//      " left join (select gridid from mrlocate where day = '" + day + "' and height = 0) m on f.gridid = m.gridid where m.gridid is null")
    //   hiveContext.sql("select * from guiyang2.mrlocate where day = '"+day+"'").foreachPartition(p =>mrlocateDao.insert2db_mrlocate(p,day,true))
    //        val gridmappingbuilding = DBHelper.Table(hiveContext, "gridmappingbuilding")
    //        gridmappingbuilding.registerTempTable("gridmappingbuilding_db")
    //
    //        val build_floor_rsrp = hiveContext.sql(" select g.BulidingID, m.height, avg(avgrsrp) from mrlocate  m " +
    //          " inner join gridmappingbuilding_db g on m.gridid = g.gridid where m.day = '"+day+"' " +
    //          " group by g.BulidingID, m.height ")
    //
    //        build_floor_rsrp.foreachPartition(p => buildfloorrsrpDao.insert2db_buildfloorrsrp(p))

//
//    hiveContext.sql(" insert into table overlaps partition(day="+day+")" +
//      "select gridid,sum(case when t.objectid is null then 0 else 1 end) as overlapd   from " +
//      "             (select * from doublemroresults where day="+day+") as d" +
//      "             left join " +
//      "                    (select a1.objectid,a1.mmeues1apid,a1.time_stamp from " +
//      "                 (select objectid,mmeues1apid,time_stamp,ltescrsrp,ltencrsrp from " +
//      "                   guizhou.mropartition where day="+day+"  and abs(ltescrsrp-ltencrsrp)<=6)  a1" +
//      "                  inner join (select * from doublemroresults where day="+day+") as a2 on a1.objectid=a2.objectid and a1.mmeues1apid=a2.mmeues1apid and a1.time_stamp=a2.time_stamp" +
//      "                  " +
//      "                  group by a1.objectid,a1.mmeues1apid,a1.time_stamp having count(1)>=3 " +
//      "                  ) as t on t.objectid=d.objectid and t.mmeues1apid=d.mmeues1apid and t.time_stamp=d.time_stamp " +
//      "             group by d.gridid")
//    hiveContext.sql("insert overwrite table lotqci " +
//      "     select gridid,sum(case when ulqci1<0 or ulqci1 is null then 0 else 1 end)+ sum(case when dlqci1<0 or dlqci1 is null then 0 else 1 end)-sum(case when ulqci1<0 or ulqci1 is null  or dlqci1<0  or dlqci1 is null then 0 else 1 end) as udtotal," +
//      "                   sum(case when ulqci1<0 or ulqci1 is null then 0 else 1 end) as uptotal," +
//      "                    sum(case when dlqci1<0 or dlqci1 is null then 0 else 1 end) as dltotal," +
//      "                     cast(sum(case when ulqci1<0 or ulqci1 is null  then 0 else ulqci1 end)/sum(case when ulqci1<0  or ulqci1 is null then 0 else 1 end) as decimal(18,4)) as uplost," +
//      "                      cast(sum(case when dlqci1<0 or dlqci1 is null then 0 else dlqci1 end)/sum(case when dlqci1<0  or dlqci1 is null  then 0 else 1 end) as decimal(18,4)) as dllost," +
//      "                      sum(case when LteScPUSCHPRBNum<0 or LteScPUSCHPRBNum is null then 0 else LteScPUSCHPRBNum end) as pusum," +
//      "                      sum(case when LteScPDSCHPRBNum<0 or LteScPDSCHPRBNum is null then 0 else LteScPDSCHPRBNum end) as pdsum " +
//      " from result where day="+day+" group by gridid ")
  }
}
