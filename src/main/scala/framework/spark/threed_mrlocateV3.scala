package framework.spark

import configuration.{AppSettings, ConfigManager, DBHelper}
import model.{mroOutFloor, resCell}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf}

/**
  * Created by ChangJiale on 2017/11/1.
  */
object threed_mrlocateV3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val logger=Logger.getLogger(threed_mrlocateV3.getClass)
    val time = args(0)
    val partitionNum = args(1).toInt
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    val conf = new SparkConf()

    //当操作到最后一个分区时，增加task并行度和shuffle并行度
    if(partitionNum==99){
      conf.set("spark.sql.shuffle.partitions", "1000")
          .set("spark.default.parallelism", "1000")
    }
    val hiveContext= AppSettings.setConf(conf)
    //动态的获取表名
    val fingerOneTableName = ConfigManager.getProperty("fingerOnePartition")
    val fingerHighTableName = ConfigManager.getProperty("fingerHighPartition")

    val mroPartitionTableName =  ConfigManager.getProperty("mroPartitionTable")
    val mroFloor = ConfigManager.getProperty("mroFloorTable")
    val mroResult = ConfigManager.getProperty("mroResultTable")
    val database=ConfigManager.getProperty("dataBaseName")
    //判断是否有当天分区表数据没有直接退出
    if(partitionNum==0){
      hiveContext.sql(s"use ${database}")
      val testData=hiveContext.sql(s"select * from ${mroPartitionTableName} where day = ${day} and hour = '${hour}' limit 1").rdd
      if(testData.isEmpty()){
        logger.error(s"SORRY NO ${day} ,${hour} 'S DATA,PLEASE GENERATE PARTITION DATA ")
        return
      }
    }
    //指纹库定位
    mroJoinFloor(hiveContext, mroFloor, mroPartitionTableName, fingerOneTableName, fingerHighTableName, day, hour, partitionNum,database)

    //计算指纹库定位和ta+aoa定位
    generateMroResult(hiveContext, mroPartitionTableName, mroFloor, mroResult, day, hour, partitionNum, fingerOneTableName,database)

    hiveContext.stop()
  }

  /**功能：将每天每个小时的数据均匀的分好100个分区后,将mro数据与指纹库每个分区去join分别计算1层和高层的数据，最终统一落到中间结果表。
    *前99个分区是join量较小的分区，所以采用group by 一次。
    * @param hiveContext hiveContext对象
    * @param MroFloor  中间结果表
    * @param mroTableName mro分区表
    * @param fingerOneTableName 指纹库一层分区表
    * @param fingerHighTableName 指纹库高层分区表
    * @param day 天 格式yyyyMMdd
    * @param hour 小时 格式 HH
    * @param partitionNum 分区值
    * @return
    */
  def mroJoinFloor(hiveContext: SparkSession, MroFloor: String, mroTableName: String, fingerOneTableName: String, fingerHighTableName: String, day: String, hour: String, partitionNum: Int, dataBase:String) = {
    hiveContext.sql(s"use ${dataBase}")
    //创建中间表
    hiveContext.sql(
      s"""
        | create table if not exists ${MroFloor} (objectid bigint, mmeues1apid string,
        |  time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int,gridid string,height int, distance int)
        | partitioned by (day string, hour string,partitionValue int)
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile
        |
      """.stripMargin)

    //判断分区是否为第一个分区，如果是就相当于重新计算了该天数据
    if (partitionNum == 0) {
      hiveContext.sql(s"alter table ${MroFloor} DROP IF EXISTS PARTITION (day = '${day} ', hour = '${hour} ')")
    }


    /**  计算mro数据与一层指纹库 栅格化
      *  mro的每个分区和指纹库的每个分区join 节省join时扫描表的范围，join的条件是主小区id 和 邻区id
      *  主要根据objectid, mmeues1apid,time_stamp 字段做分组，使用算法求出距离，并过滤掉只有一个主临join上栅格的
      *  使用开窗函数求出最小的欧式距离 从而确定mro 精准的落在最适合的一个栅格中
      */
    hiveContext.sql(
      s"""
        | insert overwrite table ${MroFloor} PARTITION (day = '${day}', hour = '${hour}',partitionValue = ${partitionNum})
        | select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.height,d2.distance
        |
        |   from (
        |    select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul,d1.GridID,
        |    d1.height, d1.distance,row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn
        |
        |          from (
        |                select c1.objectid,c1.mmeues1apid, c1.time_stamp,c1.ltescrsrp,c1.ltescrsrq,c1.ltescsinrul,c2.GridID,c2.height,
        |                sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance
        |
        |                from
        |                     (select * from ${mroTableName}  where day = '${day}' and hour = '${hour}' and partitionValue = ${partitionNum}) c1
        |                     inner join
        |                     (select GridID,ObjectID,RSRP,n_objectid,n_rsrp,height  from ${fingerOneTableName} where partitionValue = ${partitionNum}) c2
        |                     on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid)
        |
        |                group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c2.GridID, c2.height
        |                having count(1) > 1
        |               ) d1
        |       ) d2 where d2.rn = 1  DISTRIBUTE BY d2.height
      """.stripMargin)

    //计算mro与高层的数据
    hiveContext.sql(
      s"""
         | insert overwrite table ${MroFloor} PARTITION (day = '${day}', hour = '${hour}',partitionValue = ${partitionNum})
         | select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.height,d2.distance
         |
         |   from (
         |    select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul,d1.GridID,
         |    d1.height, d1.distance,row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn
         |
         |          from (
         |                select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul,c2.GridID,c2.height,
         |                sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance
         |
         |                from
         |                     (select * from ${mroTableName}  where day = '${day}' and hour = '${hour}' and partitionValue =  ${partitionNum}) c1
         |                     inner join
         |                     (select GridID,ObjectID,RSRP,n_objectid,n_rsrp,height  from ${fingerHighTableName} where partitionValue=${partitionNum}) c2
         |                     on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid)
         |
         |                group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c2.GridID, c2.height
         |                having count(1) > 1
         |               ) d1
         |       ) d2 where d2.rn = 1  DISTRIBUTE BY d2.height
      """.stripMargin)
  }

  /**
    * and sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) <=100
    */

  /**
    *
    * @param hiveContext  hiveContext对象
    * @param mroPartitionTableName  mro分区表
    * @param mroFloor    栅格化中间临时表
    * @param mroResult   栅格化最终结果表
    * @param day         天
    * @param hour       小时
    * @param partitionNum  分区值
    * @param fingerTableName  一层指纹库表
    * @return
    */
  def generateMroResult(hiveContext: SparkSession, mroPartitionTableName: String, mroFloor: String, mroResult: String, day: String, hour: String, partitionNum: Int, fingerTableName: String,dataBase:String) = {
    hiveContext.sql(s"use ${dataBase}")

    hiveContext.sql("create table if not exists " + mroResult + " (objectid bigint, mmeues1apid string," +
      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int,gridid string , height int) partitioned by (day string, hour string,partitionValue int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile")
    hiveContext.sql(
      s"""
        | create table if not exists ${mroResult} (
        |  objectid bigint, mmeues1apid string,time_stamp string, ltescrsrp int,
        |   ltescrsrq int, ltescsinrul int,gridid string , height int)
        |  partitioned by (day string, hour string,partitionValue int)
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile
      """.stripMargin)


    hiveContext.sql(
      s"""
        | insert overwrite table ${mroResult} PARTITION (day = '${day}', hour = '${hour}',partitionValue = ${partitionNum})
        |  select b.objectid,b.mmeues1apid,b.time_stamp,b.ltescrsrp,b.ltescrsrq,b.ltescsinrul,b.gridid, b.height
        |
        |         from (
        |         select a.objectid,a.mmeues1apid,a.time_stamp, a.ltescrsrp,a.ltescrsrq,a.ltescsinrul,a.gridid,a.height,a.distance,
        |         ROW_NUMBER() over (partition by a.objectid,a.mmeues1apid,a.time_stamp order by a.distance asc) rn from
        |           ${mroFloor} a where day = '${day}', hour = '${hour}',partitionValue = ${partitionNum}
        |              ) b
        |
        |   where b.rn = 1 DISTRIBUTE BY b.height
        |
      """.stripMargin)
    //将没有落地的mr数据找出来。并转换成dataFrame
    //c1表为根据objectid,mmeues1apid,time_stamp 分组 抽取出一条主小区id的mro数据与 应栅格化的数据左连接，求出没有栅格化的mro数据
    val mroOutFloorTable=hiveContext.sql(
      s"""
        |  select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c1.TA,c1.AOA
        |
        |       from
        |             (select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,
        |             a1.ltescrsrq,a1.ltescsinrul,a1.ltencrsrp,a1.ncellobjectid,a1.TA,a1.AOA
        |             from (
        |               select objectid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid,TA,AOA,
        |               row_number() over (partition by objectid, mmeues1apid,time_stamp order by ltescrsrp desc) rn
        |               from ${mroPartitionTableName}  where day = '${day}' and hour = '${hour}'and partitionValue = ${partitionNum}) a1
        |             where rn=1) c1
        |       left join
        |
        |            (select objectid,mmeues1apid,time_stamp from ${mroResult} where day = '${day}' and  hour = '${hour}' and
        |             partitionValue = ${partitionNum} ) c2
        |
        |       on (c1.objectid = c2.ObjectID and c1.mmeues1apid = c2.mmeues1apid and c1.time_stamp = c2.time_stamp)
        |
        |  where c2.objectid is null
        |
        |
        |
        |
      """.stripMargin)

    //规范化没有栅格化的mro数据
    import hiveContext.implicits._
    val mroOutFloorDF = mroOutFloorTable.map(rs => {
      mroOutFloor(rs.getAs[Int](0), rs.getAs[String](1), rs.getAs[String](2), rs.getAs[Int](3), rs.getAs[Int](4), rs.getAs[Double](5), rs.getAs[Int](6), rs.getAs[Int](7))
    }).toDF()

    //读取sqlserver中标准库的数据
    val resCellTable = DBHelper.Table(hiveContext, "tuning.res_cell")
    val resCellDF = resCellTable.map(rs => {
      resCell(rs.getAs[Int](0), rs.getAs[Double](5), rs.getAs[Double](6))
    }).toDF()
    //将没有栅格化的mr数据与与标准库数据join，通过Ta和AOA字段计算出MR采样点经纬度。
    mroOutFloorDF.join(resCellDF, mroOutFloorDF("objectid") === resCellDF("ObjectID")).select(mroOutFloorDF("objectid").as("objectid"), mroOutFloorDF("mmeues1apid").as("mmeues1apid"),
                       mroOutFloorDF("time_stamp").as("time_stamp"), mroOutFloorDF("ltescrsrp").as("ltescrsrp"), mroOutFloorDF("ltescrsrq").as("ltescrsrq"), mroOutFloorDF("ltescsinrul").as("ltescsinrul"), mroOutFloorDF("TA").as("TA"), mroOutFloorDF("AOA").as("AOA"),
                       udfFunctions.FingerLong2SamplingLong(resCellDF("Longitude"), resCellDF("Latitude"), mroOutFloorDF("TA"), mroOutFloorDF("AOA")).as("Longitude"), udfFunctions.FingereLa2SamplingLa(resCellDF("Longitude"), resCellDF("Latitude"), mroOutFloorDF("TA"), mroOutFloorDF("AOA")).as("Latitude"))
                       .createOrReplaceTempView("mroJoinCell")
    //将join好的mr数据与指纹库0层指纹库join
    //由于使用join条件为经纬度后四位原则，mro数据可能会落到多个栅格中，取一条rsrp 最好的即可


    hiveContext.sql(
      s"""
        | insert into table ${mroResult}  PARTITION (day = '${day}', hour = '${hour}',partitionValue = ${partitionNum})
        |  select d1.objectid,d1.mmeues1apid,d1.time_stamp,d1.ltescrsrp,d1.ltescrsrq,d1.ltescsinrul,d1.gridid,d1.height from
        |    from (
        |          select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,a1.ltescsinrul,a2.gridid,a2.height,
        |          ROW_NUMBER() over (partition by a1.objectid,a1.mmeues1apid,a1.time_stamp order by a1.ltescrsrp desc) rk from
        |
        |               (select * from mroJoinCell) a1
        |               inner join
        |               (select distinct GridID, Longitude, Latitude, ObjectID, height from ${fingerTableName} where partitionValue = ${partitionNum}) a2
        |               on
        |               (cast(a1.Longitude as decimal(18,4)) = cast(a2.Longitude as decimal(18,4)) and cast(a1.Latitude as decimal(18,4)) = cast(a2.Latitude as decimal(18,4)))
        |
        |         ) d1
        |   where d1.rk=1 DISTRIBUTE BY d1.height
        |
      """.stripMargin)

  }


//  /**
//    * 老代码备份
//    * @param hiveContext
//    * @param MroFloor
//    * @param mroTableName
//    * @param fingerOneTableName
//    * @param fingerHighTableName
//    * @param day
//    * @param hour
//    * @param partitionNum
//    * @param dataBase
//    * @return
//    */

//  def backUpsCode(hiveContext: SparkSession, MroFloor: String, mroTableName: String, fingerOneTableName: String, fingerHighTableName: String, day: String, hour: String, partitionNum: Int,dataBase:String)={
//
//    hiveContext.sql(" insert overwrite table " + MroFloor + " PARTITION (day = '" + day + "', hour = '" + hour + "',partitionValue = " + partitionNum + ") " +
//      //使用开窗函数求出最小的距离就是该mro数据栅格化的结果
//      " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.height,d2.distance from " +
//      " (" +
//      "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul,d1.GridID, d1.height, d1.distance, " +
//      "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
//      "   from (" +
//      //主要根据objectid, mmeues1apid,time_stamp 字段做分组，使用算法求出距离，并过滤掉小于一条的
//      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul,c2.GridID,c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
//      //mro的每个分区和指纹库的每个分区join 节省join时扫描表的范围，join的条件是主小区id 和 邻区id
//      "               (select * from " + mroTableName + " where day = '" + day + "' and hour = '" + hour + "'and partitionValue = " + partitionNum + ")  c1 inner join  " +
//      "               (select GridID,ObjectID,RSRP,n_objectid,n_rsrp,height  from " + fingerOneTableName + " where partitionValue=" + partitionNum + ") c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) " +
//
//      "          group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c2.GridID, c2.height" +
//      "          having count(1) > 1 and sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) <=100" +
//      "    ) d1" +
//      ") d2 where d2.rn = 1  DISTRIBUTE BY d2.height")
//
//
////    hiveContext.sql("insert overwrite table " + mroResult + " PARTITION (day = '" + day + "', hour = '" + hour + "',partitionValue = " + partitionNum + ") " +
////      //求出最小的距离，从而能选择出该mro到底是在那一层
////      " select b.objectid,b.mmeues1apid,b.time_stamp,b.ltescrsrp,b.ltescrsrq,b.ltescsinrul,b.gridid, b.height from " +
////      //使用开窗函数根据a.objectid,a.mmeues1apid,a.time_stamp,分组，根据距离正序排序,取最小距离确定到底在哪一层。
////      "    (select a.objectid,a.mmeues1apid,a.time_stamp, a.ltescrsrp,a.ltescrsrq,a.ltescsinrul,a.gridid,a.height,a.distance,ROW_NUMBER() over (partition by a.objectid,a.mmeues1apid,a.time_stamp order by a.distance asc) rn  from " +
////      "      " + mroFloor + " a where day = '" + day + "' and hour = '" + hour + "'and partitionValue = " + partitionNum + ") b " +
////
////      " where b.rn = 1 DISTRIBUTE BY b.height")
//
//
////    val mroOutFloorTable1 = hiveContext.sql("select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c1.TA,c1.AOA from " +
////      //c1表为根据objectid,mmeues1apid,time_stamp 分组 抽取出一条主小区id的mro数据与 应栅格化的数据左连接，求出没有栅格化的mro数据
////      "     (select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,a1.ltescsinrul,a1.ltencrsrp,a1.ncellobjectid,a1.TA,a1.AOA from " +
////      "     (select objectid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid,TA,AOA,row_number() over (partition by objectid, mmeues1apid,time_stamp order by objectid asc) rn" +
////      "     from " + mroPartitionTableName + " where day = '" + day + "' and hour = '" + hour + "'and partitionValue = " + partitionNum + ")a1 " +
////      "  where rn = 1) c1 left join " +
////      " (select objectid,mmeues1apid,time_stamp from " + mroResult + " where day = '" + day + "' and hour = '" + hour + "'and " +
////      "  partitionValue = " + partitionNum + ") c2 on (c1.objectid = c2.ObjectID and c1.mmeues1apid = c2.mmeues1apid and c1.time_stamp = c2.time_stamp) where c2.objectid is null")
//
////    hiveContext.sql("insert into table " + mroResult + " PARTITION (day = '" + day + "', hour = '" + hour + "',partitionValue = " + partitionNum + ")" +
////
////      "select d1.objectid,d1.mmeues1apid,d1.time_stamp,d1.ltescrsrp,d1.ltescrsrq,d1.ltescsinrul,d1.gridid,d1.height from " +
////      "       (select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,a1.ltescsinrul,a2.gridid,a2.height," +
////      "        ROW_NUMBER() over (partition by a1.objectid,a1.mmeues1apid,a1.time_stamp order by a1.ltescrsrp desc) rk from " +
////      //将处理好的mro数据与指纹库一层join，条件为经纬度小数点后四位原则
////      "              (select * from mroJoinCell) a1 inner join " +
////      "              (select distinct GridID, Longitude, Latitude, ObjectID, height from " + fingerTableName + " where partitionValue = " + partitionNum + ") a2 on " +
////      "              (cast(a1.Longitude as decimal(18,4)) = cast(a2.Longitude as decimal(18,4)) and cast(a1.Latitude as decimal(18,4)) = cast(a2.Latitude as decimal(18,4)))) d1 " +
////
////      "where d1.rk=1 DISTRIBUTE BY d1.height")
//
//  }

}