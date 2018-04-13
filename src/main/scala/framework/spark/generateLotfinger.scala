package framework.spark

import configuration.{AppSettings, DBHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf}

/**
  * Created by Administrator on 2017/12/12.
  */
object generateLotfinger {
  def main(args: Array[String]): Unit = {

    //给conf设置一些参数
   val hiveContext= AppSettings.setConf()
    //读取频点表
    hiveContext.sql("use anhui")
    //控制是否删除室外栅格
import hiveContext.implicits._
    val flag=false
    if(flag){
      deleteHighOutDoorGrid(hiveContext,"anhui","outfingertable")
    }
//    val frequency_point=DBHelper.Table(hiveContext,"tuning.Res_cell")
//    frequency_point.createOrReplaceTempView("rescell")
//    DBHelper.Table(hiveContext,"GridMappingBuilding").createOrReplaceTempView("GridMappingBuilding")
//    DBHelper.Table(hiveContext,"cellBuildrelation").createOrReplaceTempView("cellBuildRelation")
//    DBHelper.Table(hiveContext,"tuning.res_site").createOrReplaceTempView("res_site")
    //获取字段
    hiveContext.udf.register[Int,String]("transformObjectId",(fieds1)=>udfFunctions.transformObjectid(fieds1))
    val str="concat(google_gci,'_',google_gri) as gridid"
    val longitude="(google_gci*5)/20037508.34*180 as longitude"
    val latitude="180/pi()*(2*atan(exp((google_gri*5)/20037508.34*180*pi()/180))-pi()/2) as latitude"



//    val neighbor_finger= hiveContext.sql(
//      s"""
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_0) as n_objectid ,cast(rsrp_0 as double) as n_rsrp from outfingertable
//        |         union all
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_1) as n_objectid ,cast(rsrp_1 as double) as n_rsrp from outfingertable
//        |         union all
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_2) as n_objectid ,cast(rsrp_2 as double) as n_rsrp from outfingertable
//        |         union all
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_3) as n_objectid ,cast(rsrp_3 as double) as n_rsrp from outfingertable
//        |         union all
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_4) as n_objectid ,cast(rsrp_4 as double) as n_rsrp from outfingertable
//        |         union all
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_5) as n_objectid ,cast(rsrp_5 as double) as n_rsrp from outfingertable
//        |         union all
//        |    select ${str},${longitude},${latitude},${longitude1},${latitude1},${longitude2},${latitude2},grid_height as height,transformObjectId(antenna_6) as n_objectid ,cast(rsrp_6 as double) as n_rsrp from outfingertable
//        |
//        |
//      """.stripMargin)

    /**
      * 高级用法 等同于UNION ALL 获取邻区
      */
    val neighbor_finger= hiveContext.sql(
      s"""
        | select t.gridid,t.longitude,t.latitude,t.height,cast( split(objectid_rsrp,'&')[0] as double) as n_objectid,cast(split(objectid_rsrp,'&')[1] as double) as n_rsrp
        |                 from (
        |                       select  ${str},${longitude},${latitude},grid_height as height ,objectid_rsrp from outfingertable as d
        |                       LATERAL VIEW
        |                       explode(
        |                               split(concat_ws(',',concat(transformObjectId(antenna_0),'&',rsrp_0),concat(transformObjectId(antenna_1),'&',rsrp_1)
        |                               ,concat(transformObjectId(antenna_2),'&',rsrp_2),concat(transformObjectId(antenna_3),'&',rsrp_3)
        |                               ,concat(transformObjectId(antenna_4),'&',rsrp_4),concat(transformObjectId(antenna_5),'&',rsrp_5)
        |                               ,concat(transformObjectId(antenna_6),'&',rsrp_6)),',')
        |                               ) concattable as objectid_rsrp
        |                       ) as t
      """.stripMargin)
    neighbor_finger.createOrReplaceTempView("neighbor_finger")

     //计算主服务小区

    val server_finger=  hiveContext.sql(
        """
          | select t.objectid,t.gridid,t.longitude,t.latitude,t.rsrp,t.height
          |         from (
          |                 select a1.gridid,a1.longitude,a1.latitude,a1.n_objectid as objectid,a1.height,a1.n_rsrp as rsrp,
          |                 row_number() over (partition by a1.gridid,a1.height,a2.earfcn order by a1.n_rsrp desc) rk
          |                 from neighbor_finger a1
          |                 inner join
          |                 (select distinct objectid,earfcn from rescell) a2 on a1.n_objectid=a2.objectid
          |               ) t
          |
          | where t.rk = 1
        """.stripMargin)
    server_finger.createOrReplaceTempView("server_finger")//("finger_server")


//
//    /**
//      * 室分回填
//      */

//    val fillBack=hiveContext.sql(
//      """
//        | select t.objectid,t.gridid,t.longitude,t.latitude,(rand()- 0.5)* 20  + (-90) as rsrp,t.height
//        |          from (
//        |          select distinct f.gridid,f.longitude,f.latitude,cb.ObjectID,f.height
//        |          from               res_site rs
//        |               inner join rescell rc on rs.OID = rc.SiteOID
//        |               inner join cellBuildrelation cb on cb.ObjectID = rc.ObjectID
//        |               inner join GridMappingBuilding gb on gb.BulidingID = cb.BuildingID
//        |               inner join finger_server f on f.GridID = gb.GridID
//        |               where rs.SiteType =0
//        |               ) as t
//      """.stripMargin)
//    server_finger.unionAll(fillBack).createOrReplaceTempView("server_finger_tmp")
//    hiveContext.sql(
//      """
//        | select objectid,gridid,longitude,latitude,rsrp,height from (
//        | select * ,row_number() over(partition by gridid order by rsrp desc) rk ) t
//        | where rk=1
//      """.stripMargin).createOrReplaceTempView("server_finger")

    hiveContext.sql("create table if not exists fingerdis0 (objectid int, gridid string,longitude double,latitude double,rsrp double, n_objectid int, n_rsrp int,height int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //hiveContext.sql("create table if not exists fingerhigh (objectid int, gridid string,longitude double,latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")
//    //插入一层指纹库
    hiveContext.sql(
        """
          | insert into table fingerdis0
          |  select distinct a1.objectid,a1.gridid ,a1.longitude,a1.latitude,a1.rsrp,a2.n_objectid,a2.n_rsrp,a1.height
          |       from
          |             (select * from server_finger where height=0) a1
          |       inner join
          |             ( select * from neighbor_finger where height=0) a2
          |       on a1.gridid=a2.gridid
          |  where a1.objectid != a2.n_objectid
          |
        """.stripMargin)
    //插入高层指纹库
//    hiveContext.sql(
//      """
//        | insert into table fingerhigh
//        |         select a1.objectid,a1.gridid,a1.longitude,a1.latitude,a1.rsrp,a2.n_objectid,a2.n_rsrp,a1.height
//        |               from
//        |                     (select * from server_finger where height  != 0) a1
//        |               inner join
//        |                     ( select * from neighbor_finger where height != 0) a2
//        |               on a1.gridid = a2.gridid and a1.height=a2.height
//        |          where a1.objectid != a2.n_objectid
//        |
//      """.stripMargin)




  }


  def deleteHighOutDoorGrid(hiveContext: SparkSession,dataBaseName:String,tableName:String): Unit ={
    hiveContext.sql(s"use ${dataBaseName}")

    hiveContext.sql(
      s"""
        |
        | create table if not exists ${tableName}
        |  (grid_row_id bigint,grid_col_id bigint,google_gri bigint,google_gci int,grid_type string,
        |  grid_height string,antenna_0 string,rsrp_0 string,antenna_1 string,rsrp_1 string,
        |  antenna_2 string,rsrp_2 string,antenna_3 string,rsrp_3 string,antenna_4 string,
        |  rsrp_4 string,antenna_5 string,rsrp_5 string,antenna_6 string,rsrp_6 string)
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile
      """.stripMargin)
    DBHelper.Table(hiveContext,"GridMappingBuilding").registerTempTable("GridMappingBuilding")
    DBHelper.Table(hiveContext , "field_gz_guiyang_201801").registerTempTable("inputFingerTable")

    hiveContext.sql(
      s"""
        | insert into table ${tableName}
        |  select grid_row_id,grid_col_id,google_gri,google_gci,grid_type,grid_height,antenna_0,rsrp_0,antenna_1,
        |         rsrp_1,antenna_2,rsrp_2,antenna_3,rsrp_3,antenna_4,rsrp_4,antenna_5,rsrp_5,antenna_6,rsrp_6
        |         from (
        |               select fg.grid_row_id,fg.grid_col_id,fg.google_gri,fg.google_gci,fg.grid_type,fg.grid_height,
        |               fg.antenna_0,fg.rsrp_0,fg.antenna_1,fg.rsrp_1,fg.antenna_2,fg.rsrp_2,fg.antenna_3,fg.rsrp_3,
        |               fg.antenna_4,fg.rsrp_4,fg.antenna_5,fg.rsrp_5,fg.antenna_6,fg.rsrp_6,
        |               ROW_NUMBER() over(PARTITION BY fg.grid_col_id, fg.grid_row_id,fg.google_gri ,fg.google_gci,fg.grid_height order by fg.grid_height desc) as rn
        |               from  GridMappingBuilding gb
        |
        |               inner join
        |               (select concat(google_gci,'_',google_gri) as gridid,grid_row_id,grid_col_id,google_gri,google_gci,
        |               grid_type,grid_height,antenna_0,rsrp_0,antenna_1,rsrp_1,antenna_2,rsrp_2,antenna_3,rsrp_3,antenna_4,rsrp_4,
        |               antenna_5,rsrp_5,antenna_6,rsrp_6 from  inputFingerTable f where grid_height <> 0 ) fg
        |               on gb.GridID = fg.GridID
        |               ) gg
        |   where gg.rn = 1
      """.stripMargin)

    //将一层数据插入

    hiveContext.sql(
      s"""
        | insert into table ${tableName}
        |   select fg.grid_row_id,fg.grid_col_id,fg.google_gri,
        |   fg.google_gci,fg.grid_type,fg.grid_height,fg.antenna_0,
        |   fg.rsrp_0,fg.antenna_1,fg.rsrp_1,fg.antenna_2,fg.rsrp_2,fg.antenna_3,
        |   fg.rsrp_3,fg.antenna_4,fg.rsrp_4,fg.antenna_5,fg.rsrp_5,fg.antenna_6,fg.rsrp_6
        |   from inputFingerTable fg
        |   where grid_height = 0
      """.stripMargin)



  }

  /**
    * 老代码 备份
    *
    */
  def backUp(): Unit ={
//    val neighbor_finger=hiveContext.sql(" select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_0) as n_objectid ,cast(rsrp_0 as double) as n_rsrp from outfingertable  " +
//      " union all " +
//      " select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_1) as n_objectid,cast(rsrp_1 as double) as n_rsrp from outfingertable " +
//      " union all " +
//      " select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_2) as n_objectid,cast(rsrp_2 as double) as n_rsrp from outfingertable " +
//      " union all " +
//      " select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_3) as n_objectid,cast(rsrp_3 as double) as n_rsrp from outfingertable " +
//      " union all " +
//      " select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_4) as n_objectid,cast(rsrp_4  as double) as n_rsrp from outfingertable " +
//      " union all " +
//      " select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_5) as n_objectid,cast(rsrp_5 as double) as n_rsrp from outfingertable " +
//      " union all" +
//      " select "+str+","+longitude+","+latitude+",grid_height as height,transformObjectId(antenna_6) as n_objectid,cast(rsrp_6 as double) as n_rsrp from outfingertable ")

//    val server_finger=hiveContext.sql("select t.objectid,t.gridid,t.longitude,t.latitude,t.rsrp,t.height from " +
//      "           (select a1.gridid,a1.longitude,a1.latitude,a1.n_objectid as objectid,a1.height,a1.n_rsrp as rsrp," +
//      "               row_number() over (partition by a1.gridid,a1.height,a2.earfcn order by a1.n_rsrp desc) rk from " +
//      "           neighbor_finger a1 inner join rescell a2 on a1.n_objectid=a2.objectid) t" +
//      "    where t.rk = 1")

//    val fillBack = hiveContext.sql("select t.objectid,t.gridid,t.longitude,t.latitude,(rand()- 0.5)* 20  + (-90) as rsrp,t.height " +
//      "               from (" +
//      "               select distinct f.gridid,f.longitude,f.latitude,cb.ObjectID,f.height" +
//      "               from               res_site rs " +
//      "                     inner join rescell rc on rs.OID = rc.SiteOID" +
//      "                     inner join cellBuildrelation cb on cb.ObjectID = rc.ObjectID" +
//      "                     inner join GridMappingBuilding gb on gb.BulidingID = cb.BuildingID" +
//      "                     inner join finger_server f on f.GridID = gb.GridID" +
//      "                     where rs.SiteType =0" +
//      "                     ) as t ")


//    hiveContext.sql("insert into table finger0" +
//      "   select a1.objectid,a1.gridid ,a1.longitude,a1.latitude,a1.rsrp,a2.n_objectid,a2.n_rsrp,a1.height from" +
//      " (select * from server_finger where height=0) a1 inner join " +
//      " ( select * from neighbor_finger where height=0) a2 " +
//      " on a1.gridid=a2.gridid " +
//      "  where a1.objectid != a2.n_objectid")

//    hiveContext.sql("insert into table fingerhigh" +
//      "   select a1.objectid,a1.gridid,a1.longitude,a1.latitude,a1.rsrp,a2.n_objectid,a2.n_rsrp,a1.height from" +
//      " (select * from server_finger where height  != 0) a1 inner join " +
//      " ( select * from neighbor_finger where height != 0) a2 " +
//      " on a1.gridid = a2.gridid and a1.height=a2.height" +
//      " where a1.objectid != a2.n_objectid")
  }

}
