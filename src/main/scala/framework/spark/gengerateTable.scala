package framework.spark

import configuration.{AppSettings, ConfigManager, DBHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext}

/**
  * Created by Administrator on 2017/11/1.
  * 将mro表和指纹库表均匀的生成为新的分区表
  */
object gengerateTable {
  def main(args: Array[String]): Unit = {
    val time = args(0)
    val partitionNum = args(1).toInt
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    val hiveContext= AppSettings.setConf()
    //获取mro表名
    val mroTable = ConfigManager.getProperty("mroTable")
    //获取mro分区表名
    val mroPartitionTable = ConfigManager.getProperty("mroPartitionTable")
    //获取指纹库一层表名
    val fingerOneTableName = ConfigManager.getProperty("fingerOneTableName")
    //finger0
    //获取指纹库一层分区表表名
    val fingerOnePartition = ConfigManager.getProperty("fingerOnePartition")
    //获取指纹库高层表名
    val fingerHighTableName = ConfigManager.getProperty("fingerHighTableName")
    //获取指纹库高层分区表表名
    val fingerHighPartition = ConfigManager.getProperty("fingerHighPartition")
    //获取数据库名
    val dataBase = ConfigManager.getProperty("dataBaseName")
    val PartitionModuloValue = ConfigManager.getInteger("PartitionModuloValue")
    val flag = false
    //生成指纹库,因为不用每一次都从数据库中生成指纹库
    //生成mr分区表和指纹库分区表
   //gengeratePartitionTable(sc, hiveContext, PartitionModuloValue, day, hour, mroTable, mroPartitionTable, fingerOneTableName, fingerOnePartition, fingerHighTableName, fingerHighPartition, dataBase)
//
  }

  /**
    *
    * @param sc                   sparkContext对象
    * @param hiveContext          hiveContext对象
    * @param PartitionModuloValue 最大分区值
    * @param day                  所处理数据的天
    * @param hour                 所处理数据的小时
    * @param mroTable             mr元数据表
    * @param mroPartitionTable    mr分区表
    * @param fingerOneTableName   一层指纹库表
    * @param fingerOnePartition   一层指纹库分区表
    * @param fingerHighTableName  高层指纹库表
    * @param fingerHighPartition  高层指纹库分区表
    * @param dataBase             所用数据库
    */
  def gengeratePartitionTable(sc: SparkContext, hiveContext: SparkSession, PartitionModuloValue: Int, day: String, hour: String, mroTable: String, mroPartitionTable: String, fingerOneTableName: String, fingerOnePartition: String, fingerHighTableName: String, fingerHighPartition: String, dataBase: String): Unit = {
    hiveContext.sql(s"use ${dataBase}")
    //计算出join后每个小区id的count值,由于一层指纹库栅格多，处理速度慢，所以只求mr与一层指纹库objectid join后的数量。
    val countTable=hiveContext.sql(
      s"""
        | select c1.objectid,count(*) as objectcount
        |           from
        |           (select distinct objectid,ncellobjectid from ${mroTable} where day = '${day}' and hour = '${hour}') c1
        |                inner join
        |           (select distinct ObjectID,n_objectid from ${fingerOneTableName} ) c2
        |                on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid)
        | group by c1.objectid
      """.stripMargin)

    //注册成临时表表
    countTable.createOrReplaceTempView("counttable")
    //查出所有objectid所对应的平均值count
    val avgCount = hiveContext.sql("select avg(objectcount) from counttable").collect().map(s => s.getAs[Double](0)).head
    //查询最大count
    val maxCount=hiveContext.sql("select max(objectcount) from counttable" ).collect().map(s=>s.getAs[Long](0)).head
    //查询出大于平均值count2.5倍的小区id。
    val bigObject = countTable.filter(countTable("objectcount") >= (avgCount * 2.3) && countTable("objectcount") !=maxCount ).select(countTable("objectid")).collect().map(s => s.getAs[Long](0))
    val bigObject2 = countTable.filter( countTable("objectcount")===maxCount ).select(countTable("objectid")).collect().map(s => s.getAs[Long](0)).head

    //将查出来的所有导致数据倾斜的小区id拿出来追加到一个字符串中。
    val objectStringBuilder = new StringBuilder()

    for (objectId <- bigObject) {
      if (objectStringBuilder.isEmpty)
        objectStringBuilder.append("" + objectId + "")
      else
        objectStringBuilder.append("," + objectId + "")
    }
    objectStringBuilder.append(","+bigObject2+"")
    sc.broadcast(objectStringBuilder)

    //创建mro分区表
   hiveContext.sql(s"create table if not exists ${mroPartitionTable} (`objectid` int,`mmeues1apid` string,`time_stamp` string,`ltescrsrp` int,`ltescrsrq` int,`ltescsinrul` double, `ltencrsrp` int, `ncellobjectid` bigint,ta int,aoa int,ltescpuschprbnum int,ltescpdschprbnum int ,ulqci1 double,dlqci1 double) PARTITIONED BY (`day` string, `hour` string,`partitionValue` int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile")
   // 静态的将大于分区count2.5倍的小区id所对应的数据插入到一个分区中以便后面集中处理

   // 将剩余的小区id所对应的数据动态均匀的分配到分区中
   hiveContext.sql(s"insert overwrite table ${mroPartitionTable} partition(day = '${day}',hour = '${hour}',partitionValue) select objectid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid,ta,aoa,ltescpuschprbnum,ltescpdschprbnum,ulqci1 ,dlqci1 ,objectid%99 as partitionValue from  ${mroTable} where day='${day}' and hour='${hour}' and objectid not in (${objectStringBuilder}) DISTRIBUTE BY partitionValue")
   hiveContext.sql(s"insert overwrite table ${mroPartitionTable} partition(day = '${day}',hour = '${hour}',partitionValue=99) select objectid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid,ta,aoa,ltescpuschprbnum,ltescpdschprbnum,ulqci1 ,dlqci1 ,99 as partitionValue from  " + mroTable + " where day='" + day + "' and hour='" + hour + s"' and objectid in (${objectStringBuilder}) DISTRIBUTE BY partitionValue")
//    创建指纹库分区表
    hiveContext.sql(s"create table if not exists ${fingerOnePartition} (objectid int,gridid string, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int) partitioned by (partitionValue int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile")
   hiveContext.sql(s"create table if not exists ${fingerHighPartition} (objectid int,gridid string, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int) partitioned by (partitionValue int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile")
    //生成指纹库分区表，分区要与mro分区一致，否则分区join的时候会导致丢失数据
   hiveContext.sql(s"insert overwrite table ${fingerOnePartition} partition (partitionValue = ${PartitionModuloValue}) select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height, 99 as partitionValue from ${fingerOneTableName} where objectid in (${objectStringBuilder}) DISTRIBUTE BY partitionValue")
   hiveContext.sql(s"insert overwrite table ${fingerHighPartition} partition (partitionValue = ${PartitionModuloValue}) select objectid,  gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height, 99 as partitionValue from ${fingerHighTableName} where objectid in (${objectStringBuilder}) DISTRIBUTE BY partitionValue")
   hiveContext.sql(s"insert overwrite table ${fingerOnePartition} partition (partitionValue) select objectid,  gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height, objectid%100 as partitionValue from ${fingerOneTableName}   DISTRIBUTE BY partitionValue ")
   hiveContext.sql(s"insert overwrite table ${fingerHighPartition} partition (partitionValue) select objectid,  gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,height, objectid%100 as partitionValue from ${fingerHighTableName}    DISTRIBUTE BY partitionValue")

  }

  /** 功能：
    * 1.在sqlserver中读取数据生成指纹库
    * 2.构建新的指纹库分区表
    *
    * @param hiveContext         hiveContext对象
    * @param fingerOneTableName  一层指纹库
    * @param fingerHighTableName 高层指纹库
    */
  def generateFinger(hiveContext: SparkSession, fingerOneTableName: String, fingerHighTableName: String,database:String): Unit = {
    hiveContext.sql(s"use ${database}")
    val maxHeight = AppSettings.maxHeight
    var height = 0
    hiveContext.sql(s"create table if not exists ${fingerOneTableName} (objectid int, gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")
    hiveContext.sql(s"create table if not exists ${fingerHighTableName} (objectid int, gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int,height int)")
    //生成指纹库
    while (height <= maxHeight) {
      val fingerPrint_service = DBHelper.Table(hiveContext, s"FingerPrintDatabase_Service_${height}" )
      val fingerPrint_neighbor = DBHelper.Table(hiveContext, s"FingerPrintDatabase_NeighborCell_${height}")
      fingerPrint_service.createOrReplaceTempView(s"service_${height}")
      fingerPrint_neighbor.createOrReplaceTempView(s"neighbor_${height}" )

      val fingerTotal=hiveContext.sql(
        s"""
          | select s.GridID, s.Longitude, s.Latitude,${height} as height,s.Objectid, s.RSRP, n.ObjectID n_objectid, n.RSRP n_rsrp
          |             from
          |             service_${height} s
          |             inner join neighbor_${height} n  on s.GridID = n.ServiceGridID
        """.stripMargin)
      fingerTotal.createOrReplaceTempView("fingerTemp" + height)


      if (height == 0) {
        hiveContext.sql(s"insert overwrite table ${fingerOneTableName} select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,0 from fingerTemp" + height)
      } else {
        hiveContext.sql(s" insert into table ${fingerHighTableName} select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp,${height} as height  from fingerTemp" + height)
      }
      height = height + AppSettings.deltaHeight

    }

  }
}
