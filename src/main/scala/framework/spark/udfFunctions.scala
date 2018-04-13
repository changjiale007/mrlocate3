package framework.spark

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import utils.geoUtil

import scala.util.Random

/**
 * Created by xuximing on 2017/7/26.
 */
object udfFunctions {
  val isS1PointInCellUdf = udf((latitude: Double, longitude: Double, siteLat: Double, siteLon: Double, lteScRTTD: Double) => {
    val site_gcj = geoUtil.transform(siteLat, siteLon)
    val distance = geoUtil.getDistanceByLatAndLon(latitude, longitude, site_gcj._1, site_gcj._2)
    val rttdValue = (lteScRTTD + 4) * 78.125
    rttdValue >= distance
  })

  val TimestampDiff = udf((timestamp1: Timestamp, timestamp2: Timestamp) => {
    timestamp2.getTime - timestamp1.getTime
  })

  val TimestampAdd = udf((timestamp : Timestamp, second: Int) => {
    val time = timestamp.getTime + second * 1000

    new Timestamp(time)
  })

  val floor = udf((d:Double, i:Int) => {
    math.floor(d * math.pow(10, i)) / math.pow(10, i)
  })

  val log10 = udf((d:Double) => {
    math.log10(d)
  })
val FingerLong2SamplingLong=udf((cell_lon:Double, cell_lat:Double,ta:Int,aoa:Int)=>{
  val point = geoUtil.lonLat2Mercator(cell_lon, cell_lat)  //将经纬度转为墨卡托坐标
  val rttd = math.max(ta * 16 + Random.nextInt(15), 6)
  val angle = aoa + 90
  val lon = point._1 + (3 * math.pow(10, 5) * rttd / 30720 /2) * math.cos(angle / 180 * math.Pi)
  val lat = point._2 + (3 * math.pow(10, 5) * rttd / 30720 /2) * math.sin(angle / 180 * math.Pi)
  geoUtil.mercator2LonLat(lon, lat)._1  //墨卡托转为经纬度，即为MR采样点经纬度
})
  val FingereLa2SamplingLa=udf((cell_lon:Double, cell_lat:Double,ta:Int,aoa:Int)=>{
    val point = geoUtil.lonLat2Mercator(cell_lon, cell_lat)  //将经纬度转为墨卡托坐标
    val rttd = math.max(ta * 16 + Random.nextInt(15), 6)
    val angle = aoa + 90
    val lon = point._1 + (3 * math.pow(10, 5) * rttd / 30720 /2) * math.cos(angle / 180 * math.Pi)
    val lat = point._2 + (3 * math.pow(10, 5) * rttd / 30720 /2) * math.sin(angle / 180 * math.Pi)
    geoUtil.mercator2LonLat(lon, lat)._2  //墨卡托转为经纬度，即为MR采样点经纬度
  })

  def randomPrefixUDF(field: Long): String = {
    val random = new Random()
    val prefix = random.nextInt(20)
    prefix + "_" + field
  }

  def removePrefixUDF(field: String): String = {
    field.split("_")(1)
  }
   //转换gridid
  def  transformGridid(grid_row_id:String,grid_col_id:String):Long={
        val gridId=(grid_row_id+grid_col_id).toLong
        gridId
  }
  //转换objectid 1*256+2

  def transformObjectid(antenna:String): Int ={
    var objectid=0
    if(antenna.indexOf("_") != -1 && antenna.indexOf("a")== -1){
      objectid= antenna.split("_")(0).toInt*256+antenna.split("_")(1).toInt
      objectid
    }else{
      return 0
    }

  }
 //向量取权值
  def calculateValue(rsrpValue:Double):Int= {
      if(rsrpValue<=4){
       return 10
      } else if(rsrpValue<=8){
       return 9
      }else if(rsrpValue<=9){
        return 8
      }else if(rsrpValue<=10){
        return 7
      }else if(rsrpValue<=11){
        return 6
      }else if(rsrpValue<=12){
        return 5
      }else if(rsrpValue<=13){
        return 4
      }else if(rsrpValue<=14){
        return 3
      }
      return 0
  }

  //生成栅格id

  def generateGridid(google_gci:Int,google_gri:Long):String={
        val longitude=(google_gci*5)/20037508.34*180
        val latitude=180/math.Pi*(2*Math.atan(math.exp((google_gri*5)/20037508.34*180*math.Pi/180))-math.Pi/2)
        //将经纬度转换成墨卡托
        val tuple2=geoUtil.lonLat2Mercator(longitude, latitude)

       return ((tuple2._1/5).toLong+"_"+(tuple2._2/5).toLong)
  }
  //经纬度转web墨卡托
  def getwebmercatorfromwgs(WGSValue:Double,xytype:String):Long={
    var WMValue=0L
    var tmp=0.0
    val earthRad = 6378137.0
    if(xytype.equals("lon")){
      WMValue = Math.floor((WGSValue * Math.PI / 180.0 * earthRad)/5).toLong
    }
    if(xytype.equals("lat")){
      tmp= WGSValue * Math.PI / 180.0
      WMValue= Math.floor((earthRad / 2.0 * Math.log((1.0 + Math.sin(tmp)) / (1.0 - Math.sin(tmp))))/5).toLong
    }

    WMValue
  }

}
