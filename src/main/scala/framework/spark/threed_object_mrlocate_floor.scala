//package framework.spark
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//
///**
//  * Created by Administrator on 2017/9/22.
//  */
//object threed_object_mrlocate_floor {
//
//  Logger.getLogger("org").setLevel(Level.WARN)
//
//  def main(args: Array[String]): Unit = {
//    val time=args(0)
//    val day = time.substring(0, 8)
//    val hour = time.substring(8, 10)
//    val maxHeight = 100
//
//    val conf = new SparkConf().setAppName("threed_mrlocate")
//      .set("spark.akka.timeout", "10000")
//      .set("spark.network.timeout", "10000")
//      .set("spark.akka.askTimeout", "10000")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//       .set("spark.sql.shuffle.partitions","1000")
//      .set("spark.default.parallelism", "600")
//      .set("spark.storage.memoryFraction","0.2")
//      .set("spark.shuffle.memoryFraction","0.6")
//
//    val sc = new SparkContext(conf)
//    val hiveContext = new HiveContext(sc)
//    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
//    hiveContext.sql("set hive.exec.max.dynamic.partitions=10000;")
//    hiveContext.sql("set hive.exec.max.dynamic.partitions.pernode=10000;")
//
//
//    hiveContext.sql("create table if not exists mrlocate_floor_new (objectid bigint, mmeues1apid string," +
//      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int, gridid bigint, longitude double, latitude double, height int, distance int) partitioned by (day string, hour string)")
//    hiveContext.sql("ALTER TABLE mrlocate_floor_new DROP IF EXISTS PARTITION (day = '"+day+"', hour = '"+hour+"')")
//    hiveContext.sql("create table if not exists finger_total_all_new (gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int) partitioned by (objectid int)")
//
//
//
//
// hiveContext.sql("create table if not exists finger_total0_new (gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int) partitioned by (objectid int)")
// hiveContext.sql("create table if not exists mro_new (`enbid` bigint, `mmeues1apid` string, `time_stamp` string,`ltescrsrp` int,`ltescrsrq` int,`ltescsinrul` double,`ltencrsrp` int,`ncellobjectid` bigint) PARTITIONED BY (`day` string,`hour` string,`objectid` int) ")
//
//      hiveContext.sql("insert into table finger_total0_new partition (objectid) select gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,objectid from finger_total0 distribute by objectid ")
//     hiveContext.sql("insert into table mro_new partition(day='"+day+"',hour='"+hour+"',objectid) select enbid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid,objectid from mro where day='"+day+"' and hour='"+hour+"' distribute by objectid")
//      hiveContext.sql("insert into table finger_total_all_new partition (objectid) select gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height,objectid from finger_total_high distribute by objectid")
//
//
//
//
//
//    //将所有小区id查出来放在set集合中
//    val obJectIds=hiveContext.sql("select distinct objectid from finger_total0_new").toDF()
//    val partition: mutable.ListBuffer[Int] = getPartition(obJectIds)
//
//
//
//    //每个分区表进行join
//    for(i <- partition){
//      mro_finger_partitionJoin0(hiveContext,i,day,hour)
//    }
//
//    setResult(hiveContext,day,hour)
//
//
//
//
//  }
//
//  def mro_finger_partitionJoin0(hiveContext: HiveContext,objectid:Int,day:String,hour:String): Unit ={
//    hiveContext.sql(" insert into table mrlocate_floor_new PARTITION (day = '" + day + "', hour = '" + hour + "') " +
//      " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.longitude, d2.latitude, d2.height,d2.distance from " +
//      " (" +
//      "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul, d1.GridID, d1.longitude, d1.latitude, d1.height, d1.distance, " +
//      "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
//      "   from (" +
//      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
//      "             (select * from mro_new where day = '" + day + "' and hour = '" + hour + "'and objectid="+objectid+")  c1 inner join  " +
//      "               (select GridID, Longitude, Latitude, ObjectID,RSRP,n_objectid,n_rsrp,height  from finger_total_all_new where objectid="+objectid+") c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) " +
//      "             group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height" +
//      "             having count(1) > 1" +
//      "       ) d1 " +
//      ") d2 where d2.rn=1")
//
//    hiveContext.sql(" insert into table mrlocate_floor_new PARTITION (day = '" + day + "', hour = '" + hour + "') " +
//      " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.longitude, d2.latitude, d2.height,d2.distance from " +
//      " (" +
//      "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul, d1.GridID, d1.longitude, d1.latitude, d1.height, d1.distance, " +
//      "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
//      "   from (" +
//      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
//      "             (select * from mro_new where day = '" + day + "' and hour = '" + hour + "' and objectid="+objectid+")  c1 inner join  " +
//      "               (select GridID, Longitude, Latitude, ObjectID,RSRP,n_objectid,n_rsrp, 0 as height  from finger_total0_new where objectid="+objectid+") c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) " +
//      "             group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height" +
//      "             having count(1) > 1" +
//      "       ) d1 " +
//      ") d2 where d2.rn=1")
//
//  }
//
//
//  def setResult(hiveContext: HiveContext,day:String,hour:String): Unit ={
//    hiveContext.sql("create table if not exists mrlocate_result_new (objectid bigint, mmeues1apid string," +
//      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int, gridid bigint, longitude double, latitude double, height int) partitioned by (day string, hour string)")
//
//    //计算mro数据到底是在那一层
//    hiveContext.sql("insert overwrite table mrlocate_result_new PARTITION (day = '"+day+"', hour = '"+hour+"')" +
//      " select b.objectid,b.mmeues1apid,b.time_stamp,b.ltescrsrp,b.ltescrsrq,b.ltescsinrul,b.gridid, b.longitude, b.latitude, b.height " +
//      " from (select a.objectid,a.mmeues1apid,a.time_stamp, a.ltescrsrp,a.ltescrsrq,a.ltescsinrul,a.gridid,a.longitude, a.latitude,a.height,a.distance, " +
//      " ROW_NUMBER() over (partition by a.objectid,a.mmeues1apid,a.time_stamp order by a.distance asc) rn " +
//      " from mrlocate_floor_new a where day = '"+day+"' and hour = '"+hour+"') b where b.rn=1 ")
//  }
//
//  def getPartition(finger: DataFrame):ListBuffer[Int] ={
//    val obSet=new ListBuffer[Int]()
//    val fingers=finger.map(x=>{
//      x.getAs[Int]("objectid")
//    }).collect()
//    fingers.foreach(x=>{
//      obSet.append(x)
//    })
//    obSet
//  }
//}
