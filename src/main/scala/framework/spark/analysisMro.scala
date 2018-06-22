package framework.spark

import configuration.AppSettings

/**
  * Created by Administrator on 2018/6/20.
  */
object analysisMro {
  def main(args: Array[String]): Unit = {
    val hiveContext= AppSettings.setConf()
    val time=args(0)
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    hiveContext.sql(
      """
        | select
        |       a.eci,
        |       a.longitude,
        |       a.latitude,
        |       b.pci,
        |       b.earfcn
        |  from
        |       wcons_gis_antenna_lte_pciearfcn_1 a
        |  inner join
        |       wcons_gis_pciearfcn b
        |  on 1 = 1
        |
    """.stripMargin).createOrReplaceTempView("wcons_gis_expandpciearfcn")

    hiveContext.sql(
      """
        | select
        |     *,row_number() over( order by eci asc) ecinum
        |     from
        |     wcons_gis_antenna_lte_pciearfcn_1
      """.stripMargin).createOrReplaceTempView("wcons_gis_antenna_lte_pciearfcn_1_bak")

    //创建wcons_gis_cell_result_pciearfcn 表
    hiveContext.sql(
      """
        | drop table if EXISTS wcons_gis_cell_result_pciearfcn (
        |  eci bigint,
        |  earfcn int,
        |  pci int,
        |  reci int
        |  )
        |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      """.stripMargin)
    //注册udf函数  计算一对经纬度之间的距离
    hiveContext.udf.register[Double,Double,Double,Double,Double]("st_distance",(long1,long2,lat1,lat2)=>udfFunctions.getDistanceByLonAndLat(long1,long2,lat1,lat2))
    //插入数据
    hiveContext.sql(
      """
        | insert  overwrite table wcons_gis_cell_result_pciearfcn
        |         select t.eci,t.earfcn,t.pci,t.reci
        |         from (
        |               select e.eci,r.earfcn,r.pci,r.eci as reci,
        |               row_number() over(partition by e.eci,r.earfcn,r.pci order by st_distance(e.longitude,r.longitude,e.latitude,r.latitude) asc) num
        |               from
        |                   wcons_gis_expandpciearfcn e
        |                   inner join
        |                   wcons_gis_antenna_lte_pciearfcn_1_bak r
        |                   on e.pci = r.pci and e.earfcn = r.earfcn and r.eci != e.eci
        |               where r.ecinum > 6000 and r.ecinum <= 8000
        |               ) as t
        |          where t.num = 1
      """.stripMargin)

    //生成mr 表
    hiveContext.sql(
      """
        |CREATE EXTERNAL TABLE `mro`(
        |  `objectid` bigint,
        |  `mmeues1apid` string,
        |  `time_stamp` string,
        |  `ncellobjectid` bigint,
        |  `ltescrsrp` int,
        |  `ltescrsrq` int,
        |  `ltescsinrul` double,
        |  `ltencrsrp` int,
        |  `ta` int,
        |  `aoa` int,
        |  `LteScPUSCHPRBNum` int,
        |  `LteScPDSCHPRBNum` int,
        |  `ulqci1` double,
        |  `dlqci1` double)
        |PARTITIONED BY (
        |  `day` string,
        |  `hour` string)
        |ROW FORMAT DELIMITED
        |  FIELDS TERMINATED BY ','
      """.stripMargin)
    //读取mro临时表，mro 源文件已上传至hive
    hiveContext.sql(
      """
        | select eci,LteScRSRP,Timestamp,mmeues1apid,LteScRSRQ,LteScEarfcn,LteScPci,LteScTadv,LteScAOA,LteScSinrUL,LteScPUSCHPRBNum,LteScPDSCHPRBNum,
        |        LteScPlrULQci1 as ulqci,LteScPlrDLQci1 as dlqci,ltencrsrp from mrotable as m
        |              LATERAL VIEW
        |              explode(
        |                  split(concat_ws(',',LteNcRSRP_1,LteNcRSRP_2,LteNcRSRP_3,LteNcRSRP_4,LteNcRSRP_5,LteNcRSRP_6,
        |                  LteNcRSRP_7,LteNcRSRP_8),',')
        |                     ) concattable as ltencrsrp
        |
     """.stripMargin).createOrReplaceTempView("mro_tmp")
    //关联wcons_gis_cell_result_pciearfcn 找到邻区
    hiveContext.sql(
      s"""
        |insert overwrite table mro PARTITION (day = ${day}, hour = ${hour})
        |  select m.eci as objectid,
        |         m.mmeues1apid,
        |         m.Timestamp,
        |         c.reci as ncellobjectid,
        |         m.ltescrsrp,
        |         m.LteScRSRQ,
        |         m.LteScSinrUL,
        |         m.ltencrsrp,
        |         m.LteScTadv,
        |         m.LteScAOA,
        |         m.LteScPUSCHPRBNum,
        |         m.LteScPDSCHPRBNum,
        |         m.ulqci,
        |         m.dlqci
        |             from
        |                mro_tmp  as m
        |                inner join
        |                wcons_gis_cell_result_pciearfcn as c
        |                on m.eci = c.eci and m.LteScEarfcn = c.earfcn and m.LteScPci = n.pci
      """.stripMargin)



  }
}
