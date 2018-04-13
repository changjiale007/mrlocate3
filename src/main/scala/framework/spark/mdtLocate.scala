package framework.spark
import configuration.AppSettings
import model.mdt
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try


/**
  * Created by Administrator on 2018/3/7.
  */

object mdtLocate {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val day = args(0)
    val conf = new SparkConf()
    val spark=AppSettings.setConf(conf)
    spark.sql("use anhui")
    //读取 csv文件并注册CSV表mdt
    //readCsv(spark)
    //mdt栅格化,并生成mdt结果表 mdtresult


   //mdtLocate(spark,day)

  //计算结果统计表（栅格平均rsrp和栅格采样点数）
   //generateResult(spark,day)

  }


  /**
    *  读取csv 文件，并注册成临时表
    * @param spark
    */

  def readCsv(spark:SparkSession)={
    import spark.implicits._

    val mdt_text1 = spark.read.textFile("/user/datangdev/mdt1")

   val mdt1 = mdt_text1.map(rows=>{
      val fields=rows.split(",")
      val time_stamp=if(StringUtils.isNoneEmpty(fields(0)))fields(0) else null
      val MMEUES1APID=if(StringUtils.isNoneEmpty(fields(6)))fields(6) else null
      val objectid=if(Try(fields(7).toLong).isSuccess)fields(7).toLong else 0L
      val ltescrsrp=if(Try(fields(8).toInt).isSuccess)fields(8).toInt else 0


      val Longitude=if(Try(fields(10).toDouble).isSuccess)fields(10).toDouble else 0.0
      val Latitude=if(Try(fields(12).toDouble).isSuccess)fields(12).toDouble else 0.0
      mdt(time_stamp,MMEUES1APID,objectid,ltescrsrp,Longitude,Latitude,0)
    }).filter(s=>s.longitude >115 && s.longitude<120 && s.latitude > 30 && s.latitude <35)
    val mdt_text2 = spark.read.textFile("/user/datangdev/mdt2")
    val mdt2 = mdt_text2.map(rows=>{
      val fields=rows.split(",")
      val time_stamp=if(StringUtils.isNoneEmpty(fields(0)))fields(0) else null
      val MMEUES1APID=if(StringUtils.isNoneEmpty(fields(6)))fields(6) else null
      val objectid=if(Try(fields(7).toLong).isSuccess)fields(7).toLong else 0L
      val ltescrsrp=if(Try(fields(10).toInt).isSuccess)fields(10).toInt else 0
      val Longitude=if(Try(fields(12).toDouble).isSuccess)fields(12).toDouble else 0.0
      val Latitude=if(Try(fields(14).toDouble).isSuccess)fields(14).toDouble else 0.0
      mdt(time_stamp,MMEUES1APID,objectid,ltescrsrp,Longitude,Latitude,0)
    }).filter(s=>s.longitude >115 && s.longitude<120 && s.latitude > 30 && s.latitude <35)
    mdt1.union(mdt2).createOrReplaceTempView("mdt_temp")
    spark.sql("create table mdt (time_stamp string,MMEUES1APID string,objectid bigint,ltescrsrp int,Longitude double,Latitude double,height int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
    spark.sql("insert into table mdt select * from mdt_temp DISTRIBUTE BY 1")
  }


  def mdtLocate(spark:SparkSession,day:String)={
    // hiveContext.udf.register[Long,String]("parseLong",(fieds1)=>parseLong(fieds1))
    spark.udf.register[Long,Double,String]("getwebmercatorfromwgs",((fields1:Double,fields2:String)=>udfFunctions.getwebmercatorfromwgs(fields1,fields2)))
    spark.sql("create table mdtresult (objectid bigint,time_stamp string,MMEUES1APID string,ltescrsrp int,gridid string,longitude double,latitude double,height int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
    spark.sql(
      s"""
         | insert into table mdtresult
         | select a.objectid,a.time_stamp,a.MMEUES1APID,a.ltescrsrp,b.gridid,b.longitude,b.latitude,0 from
         |  mdt a
         |  inner join
         |  (select distinct gridid ,longitude,latitude from finger0) b
         |  on b.gridid= concat(getwebmercatorfromwgs(a.longitude,'lon'),'_',getwebmercatorfromwgs(a.latitude,'lat'))
      """.stripMargin)

  }
  def generateResult(spark:SparkSession,day:String)={
    spark.sql("create table mdtLocate (avgrsrp double,mdtpoint int,gridid string,longitude double,latitude double,height int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql(
      s"""
        | insert into table mdtLocate
        |   select avg(ltescrsrp) as avgrsrp , count(objectid,time_stamp,MMEUES1APID) as mdtpoint ,gridid,longitude,latitude,0
        |    from
        |    mdtresult
        |   group by gridid,longitude,latitude Distribute by 1
      """.stripMargin)
    //回填指纹库
    spark.sql(
      """
        |insert into table mdtLocate
        | select 0.0,0,a.gridid,a.longitude,a.latitude,0
        |  from
        |      (select distinct gridid ,longitude,latitude from finger0) a
        |       left join
        |       mdtLocate b on a.gridid = b.gridid
        |  where b.gridid is null Distribute by 1
      """.stripMargin)
  }

}
