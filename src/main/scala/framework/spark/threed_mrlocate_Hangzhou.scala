package framework.spark

import configuration.{AppSettings, ConfigManager}
import model.{mroOutFloor, resCell}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Changjiale on 2017/12/27.
  */
object threed_mrlocate_Hangzhou {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val logger=Logger.getLogger(threed_mrlocateV3.getClass)
    val p_hour = args(0)
    val p_city=args(1)
    val partitionNum = args(2).toInt

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
    //判断是否有当天分区表数据没有直接退出
    //结果合并小文件
    hiveContext.sql("set hive.merge.mapredfiles = true;")
    hiveContext.sql("set hive.merge.mapredfiles = true;")
    hiveContext.sql("set hive.merge.size.per.task = true;")
    hiveContext.setConf("hive.exec.max.dynamic.partitions","10000")
    hiveContext.setConf("hive.exec.max.dynamic.partitions.pernode","10000")
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    hiveContext.setConf("datanucleus.fixedDatastore","true")
    hiveContext.setConf("datanucleus.autoCreateSchema","false")
    hiveContext.setConf("hive.exec.compress.output","true")
    hiveContext.setConf("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec")
    hiveContext.setConf("mapred.output.compression.type","BLOCK")
    hiveContext.setConf("hive.exec.compress.intermediat","true")
    hiveContext.setConf("hive.intermediate.compression.codec","org.apache.hadoop.io.compress.SnappyCodec")
    hiveContext.setConf("hive.intermediate.compression.type","BLOCK")

    //指纹库定位
    mroJoinFloor(hiveContext, mroFloor, mroPartitionTableName, fingerOneTableName, fingerHighTableName, p_hour, p_city, partitionNum,database)

    //计算指纹库定位和ta+aoa定位
    generateMroResult(hiveContext, mroPartitionTableName, mroFloor, mroResult, p_hour, p_city, partitionNum, fingerOneTableName,database)

    sc.stop()
  }

  /**功能：将每天每个小时的数据均匀的分好100个分区后,将mro数据与指纹库每个分区去join分别计算1层和高层的数据，最终统一落到中间结果表。
    *前99个分区是join量较小的分区，所以采用group by 一次。
    * @param hiveContext hiveContext对象
    * @param MroFloor  中间结果表
    * @param mroTableName mro分区表
    * @param fingerOneTableName 指纹库一层分区表
    * @param fingerHighTableName 指纹库高层分区表
    * @param p_hour 天 格式yyyyMMdd
    * @param p_city 小时 格式 HH
    * @param partitionNum 分区值
    * @return
    */
  def mroJoinFloor(hiveContext: HiveContext, MroFloor: String, mroTableName: String, fingerOneTableName: String, fingerHighTableName: String, p_hour: String, p_city: String, partitionNum: Int,dataBase:String) = {
    hiveContext.sql("use "+dataBase+"")

    hiveContext.sql("create table if not exists mroresult (objectid bigint, mmeues1apid string," +
      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int,gridid bigint,height int) partitioned by (p_hour string, p_city string,partitionValue int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as orcfile")
    //判断分区是否为第一个分区，如果是就相当于重新计算了该分区
    if (partitionNum == 0) {
      //如果分区值等于0，那么就意味着重新计算这一天的数据。
      hiveContext.sql("alter table mroresult DROP IF EXISTS PARTITION (day = '" + p_hour + "', hour = '" + p_city + "')")
    }

    hiveContext.sql(" insert overwrite table mroresult PARTITION (day = '" + p_hour + "', hour = '" + p_city + "',partitionValue = " + partitionNum + ") " +
      //使用开窗函数求出最小的距离就是该mro数据栅格化的结果
      " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.height from " +
      " (" +
      "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul,d1.GridID, d1.height, " +
      "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
      "   from (" +
      //主要根据objectid, mmeues1apid,time_stamp 字段做分组，使用算法求出距离，并过滤掉小于一条的
      "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul,c2.GridID,c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
      //mro的每个分区和指纹库的每个分区join 节省join时扫描表的范围，join的条件是主小区id 和 邻区id
      "               (select * from " + mroTableName + " where day = '" + p_hour + "' and hour = '" + p_city + "'and partitionValue = " + partitionNum + ")  c1 inner join  " +
      "               (select GridID,ObjectID,RSRP,n_objectid,n_rsrp,height  from " + fingerOneTableName + " where partitionValue=" + partitionNum + ") c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) " +

      "          group by c1.objectid, c1.mmeues1apid, c1.time_stamp, c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c2.GridID, c2.height" +
      "          having count(1) > 1" +
      "    ) d1 " +
      ") d2 where d2.rn = 1  DISTRIBUTE BY d2.height")

  }

  /**
    *
    * @param hiveContext  hiveContext对象
    * @param mroPartitionTableName  mro分区表
    * @param mroFloor    栅格化中间临时表
    * @param mroResult   栅格化最终结果表
    * @param p_hour         天
    * @param p_city       小时
    * @param partitionNum  分区值
    * @param fingerTableName  一层指纹库表
    * @return
    */
  def generateMroResult(hiveContext: HiveContext, mroPartitionTableName: String, mroFloor: String, mroResult: String, p_hour: String, p_city: String, partitionNum: Int, fingerTableName: String,dataBase:String) = {
    hiveContext.sql("use "+dataBase+"")


    //将没有落地的mr数据找出来。并转换成dataFrame
    val mroOutFloorTable = hiveContext.sql("select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c1.TA,c1.AOA from " +
      //c1表为根据objectid,mmeues1apid,time_stamp 分组 抽取出一条主小区id的mro数据与 应栅格化的数据左连接，求出没有栅格化的mro数据
      "     (select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,a1.ltescsinrul,a1.ltencrsrp,a1.ncellobjectid,a1.TA,a1.AOA from " +
      "     (select objectid,mmeues1apid,time_stamp,ltescrsrp,ltescrsrq,ltescsinrul,ltencrsrp,ncellobjectid,TA,AOA,row_number() over (partition by objectid, mmeues1apid,time_stamp order by objectid asc) rn" +
      "     from " + mroPartitionTableName + " where day = '" + p_hour + "' and hour = '" + p_city + "'and partitionValue = " + partitionNum + ")a1 " +
      "  where rn = 1) c1 left join " +
      " (select objectid,mmeues1apid,time_stamp from mroresult where day = '" + p_hour + "' and hour = '" + p_city + "'and " +
      "  partitionValue = " + partitionNum + ") c2 on (c1.objectid = c2.ObjectID and c1.mmeues1apid = c2.mmeues1apid and c1.time_stamp = c2.time_stamp) where c2.objectid is null")
    //规范化没有栅格化的mro数据
    import   hiveContext.implicits._
    val mroOutFloorDF = mroOutFloorTable.map(rs => {
      mroOutFloor(rs.getAs[Int](0), rs.getAs[String](1), rs.getAs[String](2), rs.getAs[Int](3), rs.getAs[Int](4), rs.getAs[Double](5), rs.getAs[Int](6), rs.getAs[Int](7))
    }).toDF()
    //读取res_cell表
    val resCellTable = hiveContext.sql("select objectid,longitude,latitude from rc_hive_db.res_cell")
    val resCellDF = resCellTable.map(rs => {
      resCell(rs.getAs[Int](0), rs.getAs[Double](5), rs.getAs[Double](6))
    }).toDF()
    //将没有栅格化的mr数据与与标准库数据join，通过Ta和AOA字段计算出MR采样点经纬度。
    mroOutFloorDF.join(resCellDF, mroOutFloorDF("objectid") === resCellDF("ObjectID")).select(mroOutFloorDF("objectid").as("objectid"), mroOutFloorDF("mmeues1apid").as("mmeues1apid"),
      mroOutFloorDF("time_stamp").as("time_stamp"), mroOutFloorDF("ltescrsrp").as("ltescrsrp"), mroOutFloorDF("ltescrsrq").as("ltescrsrq"), mroOutFloorDF("ltescsinrul").as("ltescsinrul"), mroOutFloorDF("TA").as("TA"), mroOutFloorDF("AOA").as("AOA"),
      udfFunctions.FingerLong2SamplingLong(resCellDF("Longitude"), resCellDF("Latitude"), mroOutFloorDF("TA"), mroOutFloorDF("AOA")).as("Longitude"), udfFunctions.FingereLa2SamplingLa(resCellDF("Longitude"), resCellDF("Latitude"), mroOutFloorDF("TA"), mroOutFloorDF("AOA")).as("Latitude"))
      .registerTempTable("mroJoinCell")
    //将join好的mr数据与指纹库0层指纹库join
    hiveContext.sql("insert into table mroresult PARTITION (day = '" + p_hour + "', hour = '" + p_city + "',partitionValue = " + partitionNum + ")" +
      //由于使用join条件为经纬度后四位原则，mro数据可能会落到多个栅格中，取一条即可
      "select d1.objectid,d1.mmeues1apid,d1.time_stamp,d1.ltescrsrp,d1.ltescrsrq,d1.ltescsinrul,d1.gridid,d1.height from " +
      "       (select a1.objectid,a1.mmeues1apid,a1.time_stamp,a1.ltescrsrp,a1.ltescrsrq,a1.ltescsinrul,a2.gridid,a2.height," +
      "        ROW_NUMBER() over (partition by a1.objectid,a1.mmeues1apid,a1.time_stamp order by a2.GridID asc) rk from " +
      //将处理好的mro数据与指纹库一层join，条件为经纬度小数点后四位原则
      "              (select * from mroJoinCell) a1 inner join " +
      "              (select GridID, Longitude, Latitude, ObjectID, height from finger0partition where partitionValue = " + partitionNum + ") a2 on " +
      "              (cast(a1.Longitude as decimal(18,4)) = cast(a2.Longitude as decimal(18,4)) and cast(a1.Latitude as decimal(18,4)) = cast(a2.Latitude as decimal(18,4)))) d1 " +

      "where d1.rk=1 DISTRIBUTE BY d1.height")

  }
}
