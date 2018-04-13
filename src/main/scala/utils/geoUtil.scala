package utils

object geoUtil {

  val a = 6378245.0
  val ee = 0.00669342162296594323

  // WGS-84(World Geodetic System ) 到 GCJ-02(Mars Geodetic System) 的转换
  def transform(wgLat:Double, wgLon:Double) : (Double, Double) =
  {
    var mgLat:Double = 0
    var mgLon:Double = 0
    if (outOfChina(wgLat, wgLon))  //不再中国坐标范围
    {
      mgLat = wgLat
      mgLon = wgLon
    }
    else
    {
      val radLat = wgLat / 180.0 * math.Pi
      var magic = math.sin(radLat)
      magic = 1 - ee * magic * magic
      val sqrtMagic = Math.sqrt(magic)
      val dLat = (transformLat(wgLon - 105.0, wgLat - 35.0) * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * math.Pi)
      val dLon = (transformLon(wgLon - 105.0, wgLat - 35.0) * 180.0) / (a / sqrtMagic * math.cos(radLat) * math.Pi)
      mgLat = wgLat + dLat
      mgLon = wgLon + dLon
    }
    (mgLat, mgLon)
  }

  def transformLat(x:Double, y:Double) : Double =
  {
    var ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(math.abs(x))
    ret += (20.0 * math.sin(6.0 * x * math.Pi) + 20.0 * math.sin(2.0 * x * math.Pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * math.Pi) + 40.0 * math.sin(y / 3.0 * math.Pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * math.Pi) + 320 * math.sin(y * math.Pi / 30.0)) * 2.0 / 3.0
    return ret
  }

  def transformLon(x:Double, y:Double) : Double =
  {
    var ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(math.abs(x))
    ret += (20.0 * math.sin(6.0 * x * math.Pi) + 20.0 * math.sin(2.0 * x * math.Pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * math.Pi) + 40.0 * math.sin(x / 3.0 * math.Pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * math.Pi) + 300.0 * math.sin(x / 30.0 * math.Pi)) * 2.0 / 3.0
    ret
  }


  def outOfChina(lat:Double, lon:Double) : Boolean =
  {
    if (lon < 72.004 || lon > 137.8347) {
      return true
    }
    if (lat < 0.8293 || lat > 55.8271) {
      return true
    }
    false
  }

  def lonLat2Mercator(lon:Double, lat:Double) : (Double, Double) = {
    val x = lon * 20037508.34 / 180
    var y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180)
    y = y * 20037508.34 / 180
    (x, y)
  }

  def mercator2LonLat(x:Double, y:Double) : (Double, Double) = {
    val x1 = x/20037508.34*180
    var y1 = y/20037508.34*180
    y1 = 180 /Math.PI * (2 * Math.atan(math.exp(y1 * math.Pi /180)) - math.Pi / 2)
    (x1, y1)
  }


  def getDistanceByLatAndLon(lat:Double, lon:Double, lat1:Double, lon1:Double):Double = {
    val mercator = lonLat2Mercator(lon, lat)
    val mercator1 = lonLat2Mercator(lon1, lat1)
    math.sqrt((mercator._1 - mercator1._1) * (mercator._1 - mercator1._1) + (mercator._2 - mercator1._2) * (mercator._2 - mercator1._2))
  }

  def getDistanceByMercator(lat:Double, lon:Double, lat1:Double, lon1:Double):Double = {
    math.sqrt((lat-lat1) * (lat-lat1) + (lon-lon1) * (lon-lon1))
  }

  def ComputeGridIndex(lat:Double, lon:Double, maxLat:Double, minLon:Double, step:Int, columnCount:Int) : Long = {
    val mercator = lonLat2Mercator(lon, lat)
    val mercator1 = lonLat2Mercator(minLon, maxLat)

    val x = (mercator._1 - mercator1._1).toInt/step
    val y = (mercator1._2 - mercator._2).toInt/step

    y * columnCount + x + 1
  }

  //GCJ-02 to BD-09
  def bd_encrypt(gg_lon:Double, gg_lat:Double) : (Double, Double) =
  {
    val x = gg_lon
    val y = gg_lat
    val z = math.sqrt(x * x + y * y) + 0.00002 * math.sin(y * math.Pi)
    val theta = math.atan2(y, x) + 0.000003 * math.cos(x * math.Pi)

    val bd_lon = z * math.cos(theta) + 0.0065
    val bd_lat = z * math.sin(theta) + 0.006

    (bd_lon, bd_lat)
  }

  //BD-09 to GCJ-02
  def bd_decrypt(bd_lat:Double, bd_lon:Double) : (Double,Double) =
  {
    val x = bd_lon - 0.0065
    val y = bd_lat - 0.006
    val z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * math.Pi)
    val theta = math.atan2(y, x) - 0.000003 * math.cos(x * math.Pi)

    val gg_lon = z * math.cos(theta)
    val gg_lat = z * math.sin(theta)
    (gg_lat, gg_lon)
  }


}
