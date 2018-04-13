package framework.spark

import configuration.AppSettings

/**
  * Created by Administrator on 2018/3/8.
  */
object mdtAdJustFinger {
  def main(args: Array[String]): Unit = {
    val spark=AppSettings.setConf()
    //由于mdt栅格化结果已经生成，回填指纹库即可
    spark.sql("use anhui")
    spark.sql("create table mdtFinger0 (objectid bigint, gridid string,longitude double,latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")

    spark.sql(
      """
        |insert into table mdtFinger0
        | select
        |   a.objectid as objectid ,
        |   case when(b.gridid is null then a.gridid else b.gridid end) as gridid ,
        |   case when(b.longitude is null then a.longitude else b.longitude end) as longitude,
        |   case when(b.latitude is null then a.latitude else b.latitude end) as latitude,
        |   case when(b.avgrsrp is null then a.rsrp else b.avgrsrp end) as rsrp,
        |   a.n_objectid,a.n_rsrp,a.height
        |   from finger0 a
        |   left join
        |   mdtLocate b on a.gridid = b.gridid
      """.stripMargin)

    //校准高层指纹库

//    spark.sql("create table mdtFingerHigh(objectid int, gridid string,longitude double,latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")
//
//    spark.sql(
//      """
//        |insert into table mdtFingerHigh
//        | select
//        |   case when(b.objectid is null then a.objectid else b.objectid) as objectid,
//        |   case when(b.gridid is null then a.gridid else b.gridid ) as gridid ,
//        |   case when(b.longitude is null then a.longitude else b.longitude ) as longitude,
//        |   case when(b.latitude is null then a.latitude else b.latitude ) as latitude,
//        |   case when(b.avgrsrp is null then a.rsrp else b.avgrsrp ) as rsrp,
//        |   a.n_objectid,a.n_rsrp,a.height
//        |   from fingerhigh a
//        |   left join
//        |   mdtLocate b on a.gridid = b.gridid
//      """.stripMargin)
  }
}
