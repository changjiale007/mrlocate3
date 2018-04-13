//package framework.spark
//
//import configuration.{AppSettings, ConfigManager, DBHelper}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
//  * Created by ChangJiale on 2017/9/26.
//  */
//object threed_mrlocate {
//
//  def main(args: Array[String]): Unit = {
//    val time = args(0)
//    val partitionNum = args(1).toInt
//    val day = time.substring(0, 8)
//    val hour = time.substring(8, 10)
//    val conf = new SparkConf().setAppName("threed_mrlocate")
//      .set("spark.akka.timeout", "10000")
//      .set("spark.network.timeout", "10000")
//      .set("spark.akka.askTimeout", "10000")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      //.set("spark.sql.shuffle.partitions", "1000")
//      //.set("spark.default.parallelism", "500")
//      .set("spark.storage.memoryFraction", "0.2")
//      .set("spark.shuffle.memoryFraction", "0.6")
//      .set("spark.port.maxRetries", "100")
//    val sc = new SparkContext(conf)
//    val hiveContext = new HiveContext(sc)
//    //是否重新生成指纹库，只需要将标签改成false
//    val fingerFlag = true
//    val mroFlag = false
//    //从配置文件中获取指纹库、mro、mroresult、mrofloor表的表名
//    val fingerOneTableName = ConfigManager.getProperty("fingerOneTableName")
//    val fingerHighTableName = ConfigManager.getProperty("fingerHighTableName")
//    val mroPartitionTableName = ConfigManager.getProperty("mroPartitionTable")
//    val mroFloor = ConfigManager.getProperty("mroFloorTable")
//    val mroResult = ConfigManager.getProperty("mroResultTable")
//    //从配置文件中获取文件数
//    val PartitionModuloValue = ConfigManager.getProperty("PartitionModuloValue").toInt
//
//    //是否要生成新的指纹库。
//    if (fingerFlag) {
//      generateFinger(hiveContext, PartitionModuloValue, fingerOneTableName, fingerHighTableName)
//    }
//    //是否生成新的mro分区
//    if (mroFlag) {
//      generateMroPartition(hiveContext, mroPartitionTableName, PartitionModuloValue, day: String, hour: String)
//    }
//
//    //join生成落地表,
//    //mroJoinFloor(hiveContext, mroFloor, mroPartitionTableName, fingerOneTableName, fingerHighTableName, day, hour, partitionNum)
//    //计算出结果
//    //generateMroResult(hiveContext, mroFloor, mroResult, day, hour,partitionNum)
//
//  }
//
//  /** 功能：
//    * 生成mro分区表
//    *
//    * @param hiveContext
//    * @param tableName            mro分区表名
//    * @param PartitionModuloValue 分区规则值  （objectid%PartitionModuloValue）
//    * @param day                  时间
//    * @param hour                 小时
//    * @return
//    */
//  def generateMroPartition(hiveContext: HiveContext, tableName: String, PartitionModuloValue: Int, day: String, hour: String) = {
//    //创建mro分区表
//    hiveContext.sql("create table if not exists " + tableName + "(`enbid` bigint, `objectid` int,`mmeues1apid` string,`time_stamp` string,`ltescrsrp` int,`ltescrsrq` int,`ltescsinrul` double, `ltencrsrp` int, `ncellobjectid` bigint) PARTITIONED BY (`day` string, `hour` string,`partitionValue` int)")
//    //采用动静态分区插入数据。DISTRIBUTE BY 保证相同类型的key在一个reduce处理，避免产生过多的小文件。
//    hiveContext.sql("insert overwrite table " + tableName + " partition(day='" + day + "',hour='" + hour + "',partitionValue) select enbid,objectid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid, objectid%" + PartitionModuloValue + " as partitionValue from  mro where day='" + day + "' and hour='" + hour + "' DISTRIBUTE BY partitionValue")
//
//  }
//
//  /** 功能：
//    * 1.在sqlserver中读取数据生成指纹库
//    * 2.构建新的指纹库分区表
//    *
//    * @param hiveContext
//    * @param PartitionModuloValue 分区规则值  （objectid%PartitionModuloValue）
//    * @param fingerOneTableName   一层指纹库
//    * @param fingerHighTableName  高层指纹库
//    */
//  def generateFinger(hiveContext: HiveContext, PartitionModuloValue: Int, fingerOneTableName: String, fingerHighTableName: String): Unit = {
//    hiveContext.sql("use mrofinger")
//    val maxHeight = AppSettings.maxHeight
//    var height = 0
//
//    hiveContext.sql("create table if not exists " + fingerOneTableName + " (objectid int,gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int) partitioned by (partitionValue int)")
//    hiveContext.sql("create table if not exists " + fingerHighTableName + " (objectid int,gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int) partitioned by (partitionValue int)")
//    hiveContext.sql("create table if not exists " + fingerOneTableName + "_tmp (objectid int, gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")
//    hiveContext.sql("create table if not exists " + fingerHighTableName + "_tmp (objectid int, gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")
//    //生成指纹库
//    while (height <= 0) {
//
//      val fingerPrint_service = DBHelper.Table(hiveContext, "FingerPrintDatabase_Service")
//      val fingerPrint_neighbor = DBHelper.Table(hiveContext, "FingerPrintDatabase_NeighborCell" )
//      fingerPrint_service.registerTempTable("service_" + height)
//      fingerPrint_neighbor.registerTempTable("neighbor_" + height)
//      val fingerTotal = hiveContext.sql("select s.GridID, s.Longitude, s.Latitude," + height + " height, s.Objectid, s.RSRP, n.ObjectID n_objectid, n.RSRP n_rsrp " +
//        "from service_" + height + " s inner join neighbor_" + height + " n " +
//        " on s.GridID = n.ServiceGridID")
//      fingerTotal.registerTempTable("finger" + height)
//      if (height == 0) {
//        hiveContext.sql("insert overwrite table testlotfinger0 select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,0 from finger" + height)
//      } else {
//        //hiveContext.sql("insert overwrite table " + fingerHighTableName + "_tmp select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp," + height + "  from finger" + height)
//      }
//      height = height + AppSettings.deltaHeight
//    }
//    //将数据插入到分区表中
//    hiveContext.sql("insert overwrite table testlotfinger0partition partition (partitionValue) select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height, objectid%100 as partitionValue from testlotfinger0 DISTRIBUTE BY partitionValue ")
//    //hiveContext.sql("insert overwrite table " + fingerHighTableName + " partition (partitionValue) select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height, objectid%" + PartitionModuloValue + " as partitionValue from finger_total_high DISTRIBUTE BY partitionValue")
//  }
//
//  /** 功能：
//    * 1.创建mro数据落地表
//    * 2.将mro数据分别于指纹库高层、低层join按照分区。
//    *
//    * @param hiveContext
//    * @param MroFloor            mro数据落在指纹库中的表
//    * @param mroTableName        mro 分区表
//    * @param fingerOneTableName  一层指纹库
//    * @param fingerHighTableName 高层指纹库
//    * @param day                 天
//    * @param hour                小时
//    * @param partitionNum        所有分区
//    */
//  def mroJoinFloor(hiveContext: HiveContext, MroFloor: String, mroTableName: String, fingerOneTableName: String, fingerHighTableName: String, day: String, hour: String, partitionNum: Int) = {
//    hiveContext.sql("use mrofinger")
//    hiveContext.sql("create table if not exists " + MroFloor + " (objectid bigint, mmeues1apid string," +
//      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int, gridid bigint, longitude double, latitude double, height int, distance int) partitioned by (day string, hour string, partitionValue int)")
//    //判断分区是否为第一个分区，如果是就相当于重新计算了该分区
//    if (partitionNum == 0) {
//      hiveContext.sql("ALTER TABLE " + MroFloor + " DROP IF EXISTS PARTITION (day = '" + day + "', hour = '" + hour + "')")
//    }
//
//    hiveContext.sql(" insert overwrite table " + MroFloor + " PARTITION (day = '" + day + "', hour = '" + hour + "',partitionValue="+partitionNum+") " +
//      " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.longitude, d2.latitude, d2.height,d2.distance from " +
//      " (" +
//      "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul, d1.GridID, d1.longitude, d1.latitude, d1.height, d1.distance, " +
//      "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
//      "   from (" +
//      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
//      "             (select * from " + mroTableName + " where day = '" + day + "' and hour = '" + hour + "'and partitionValue=" + partitionNum + ")  c1 inner join  " +
//      "               (select GridID, Longitude, Latitude, ObjectID,RSRP,n_objectid,n_rsrp,height  from " + fingerOneTableName + " where partitionValue=" + partitionNum + ") c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) " +
//      "             group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height" +
//      "             having count(1) > 1" +
//      "       ) d1 " +
//      ") d2 where d2.rn=1")
//
////    hiveContext.sql(" insert into table " + MroFloor + " PARTITION (day = '" + day + "', hour = '" + hour + "',partitionValue="+partitionNum+") " +
//    //      " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.longitude, d2.latitude, d2.height,d2.distance from " +
//    //      " (" +
//    //      "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul, d1.GridID, d1.longitude, d1.latitude, d1.height, d1.distance, " +
//    //      "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
//    //      "   from (" +
//    //      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
//    //      "             (select * from " + mroTableName + " where day = '" + day + "' and hour = '" + hour + "' and partitionValue=" + partitionNum + ")  c1 inner join  " +
//    //      "               (select GridID, Longitude, Latitude, ObjectID,RSRP,n_objectid,n_rsrp, height  from " + fingerHighTableName + " where partitionValue=" + partitionNum + ") c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) " +
//    //      "             group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height" +
//    //      "             having count(1) > 1" +
//    //      "       ) d1 " +
//    //      ") d2 where d2.rn=1")
//
//  }
//
//  /** 功能：
//    * 1.创建最终结果分区表
//    * 2.查看mro数据到底是在哪一层。
//    *
//    * @param hiveContext
//    * @param mroFloor  mro落地表
//    * @param mroResult mro最终落地结果表
//    * @param day       天
//    * @param hour      小时
//    * @return
//    */
//  def generateMroResult(hiveContext: HiveContext, mroFloor: String, mroResult: String, day: String, hour: String,partitionNum:Int) = {
//    hiveContext.sql("use mrofinger")
//    hiveContext.sql("create table if not exists " + mroResult + " (objectid bigint, mmeues1apid string," +
//      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int, gridid bigint, longitude double, latitude double, height int) partitioned by (day string, hour string, partitionValue int)")
//    hiveContext.sql("insert overwrite table " + mroResult + " PARTITION (day = '" + day + "', hour = '" + hour + "',partitionValue="+partitionNum+")" +
//      " select b.objectid,b.mmeues1apid,b.time_stamp,b.ltescrsrp,b.ltescrsrq,b.ltescsinrul,b.gridid, b.longitude, b.latitude, b.height " +
//      " from (select a.objectid,a.mmeues1apid,a.time_stamp, a.ltescrsrp,a.ltescrsrq,a.ltescsinrul,a.gridid,a.longitude, a.latitude,a.height,a.distance, " +
//      " ROW_NUMBER() over (partition by a.objectid,a.mmeues1apid,a.time_stamp order by a.distance asc) rn " +
//      " from " + mroFloor + " a where day = '" + day + "' and hour = '" + hour + "' and partitionValue="+partitionNum+") b where b.rn=1 ")
//  }
//}
