package utils

import scala.util.Try
import scala.util.control.Breaks._

/**
 * Created by xuximing on 2017/1/15.
 */
object httpParser {
  def parse_http_context(host: String, http_context: String): (Boolean, String, String, String, String, String) = {
    if (host == null || http_context == null) {
      return (false, "", "", "", "", "")
    }

    var positionType: String = ""
    var success = false
    var lnglatType = ""
    var longitude = ""
    var latitude = ""
    var radius = ""
    val lowerCaseHost = host.toLowerCase().trim()
    val lowerCaseHttp_Content = http_context.toLowerCase()
    //    1. alipay.apilocate.amap.com
    //    apilocate.amap.com
    //    kdtaxi.apilocate.amap.com
    //    m5.amap.com
    //    taobao.apilocate.amap.com
    //    <cenx>120.2084196</cenx><ceny>30.207982</ceny><radius>25</radius>
    //    --<?xml version="1.0" encoding="UTF-8" ?><Cell_Req Ver="4.2.0"><BIZ></BIZ><HDA Version="4.2.0" SuccessCode="1"></HDA><DRA><apiTime>1476963850376</apiTime><coord>1</coord><retype>-5</retype><citycode>0571</citycode><adcode>330108</adcode><cenx>120.2084196</cenx><ceny>30.207982</ceny><radius>25</radius><desc><![CDATA[????????? ????????? ????????? ????????? ??????DQ????????????(?????????????????????)]]></desc><revergeo><country><![CDATA[??????]]></country><province><![CDATA[?????????]]></province><city><![CDATA[?????????]]></city><district><![CDATA[?????????]]></district><road><![CDATA[?????????]]>
    //    //取经纬度及其后radius字段。
    //    //火星坐标
    if (lowerCaseHost.equals("alipay.apilocate.amap.com")
      || lowerCaseHost.equals("apilocate.amap.com")
      || lowerCaseHost.equals("kdtaxi.apilocate.amap.com")
      || lowerCaseHost.equals("m5.amap.com")
      || lowerCaseHost.equals("taobao.apilocate.amap.com")) {
      val indexCenxBegin = lowerCaseHttp_Content.indexOf("<cenx>")
      val indexCenxEnd = lowerCaseHttp_Content.indexOf("</cenx>")
      val indexCenyBegin = lowerCaseHttp_Content.indexOf("<ceny>")
      val indexCenyEnd = lowerCaseHttp_Content.indexOf("</ceny>")
      val indexRadiusBegin = lowerCaseHttp_Content.indexOf("<radius>")
      val indexRadiusEnd = lowerCaseHttp_Content.indexOf("</radius>")
      if (indexCenxBegin != -1 && indexCenxEnd != -1
        && indexCenyBegin != -1 && indexCenyEnd != -1) {
        success = true
        longitude = lowerCaseHttp_Content.substring(indexCenxBegin + "<cenx>".length, indexCenxEnd).trim()
        latitude = lowerCaseHttp_Content.substring(indexCenyBegin + "<ceny>".length, indexCenyEnd).trim()
        if (indexRadiusBegin != -1 && indexRadiusEnd != -1) {
          radius = lowerCaseHttp_Content.substring(indexRadiusBegin + "<radius>".length, indexRadiusEnd).trim()
        }
        lnglatType = "BD"
      }
      //7. m5.amap.com
      //    "y": "30.206281", "x": "120.145655",
      //    --{"code": "1", "timestamp": "1476963846.31", "tip_list": [{"tip": {"category": "140100", "poi_tag": "<font color=#666666>?????????</font>", "name": "????????????????????????", "district": "???????????????????????????", "ignore_district": "0", "adcode": "330102", "column": "3", "rank": "13500782.025024", "datatype_spec": "0", "f_nona": "other", "datatype": "0", "child_nodes": [{"category": "991401", "name": "????????????????????????(?????????)", "datatype": "0", "adcode": "330102", "datatype_spec": "0", "y": "30.206281", "x": "120.145655", "shortname": "?????????", "poiid": "B0FFGAQZD8"}], "x_
      //    火星坐标
      if (success == false && lowerCaseHost.equals("m5.amap.com")) {
        val indexLng = lowerCaseHttp_Content.indexOf("\"x\"")
        val indexLat = lowerCaseHttp_Content.indexOf("\"y\"")
        if (indexLng != -1 && indexLat != -1) {
          var splitstr: String = "\\,|\\{|\\}"
          var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
          var tempItem: String = ""
          lnglatType = "GCJ02"
          success = true
          for (uriItem <- uriItems) {
            tempItem = uriItem.trim()
            if (tempItem.startsWith("\"x\":")) {
              longitude = tempItem.replace("\"x\":", "").trim()
            } else if (tempItem.startsWith("\"y\":")) {
              latitude = tempItem.replace("\"y\":", "").trim()
            } else if (tempItem.startsWith("\"radius\":")) {
              radius = tempItem.replace("\"radius\":", "").trim()
            }
          }
        }
      }
    }
    //    2. api.map.baidu.com
    //    "result":{"location":{"lng":120.25088311933617,"lat":30.310684375444877},
    //    "confidence":25
    //     --renderReverse&&renderReverse({"status":0,"result":{"location":{"lng":120.25088311933617,"lat":30.310684375444877},"formatted_address":"???????????????????????????????????????","business":"","addressComponent":{"country":"??????","country_code":0,"province":"?????????","city":"?????????","district":"?????????","adcode":"330104","street":"????????????","street_number":"","direction":"","distance":""},"pois":[{"addr":"????????????5277???","cp":" ","direction":"???","distance":"68","name":"????????????????????????????????????","poiType":"????????????","point":{"x":120.25084961536486,"y":30.3112150
    //    如果一个CONTENT中包含如上特征的规则，则取location，放弃后面的point或者其他类型可能存在的经纬度；
    //    第二个特征值：confidence为可信度，表示经纬度坐标的准确度即等效radius字段，一般不包含该字段，但如果检测包含该字段，则需要将confidence字段保存。
    //    前者LOCATION为定位位置，后者POINT为搜索周边POI信息所在位置。
    //    百度坐标
    //3. ***********api.map.baidu.com******************
    //    "point":{"x":120.20848914102,"y":30.327836489696}
    //    --{"content":{"address":"????????????????????????????????????","address_detail":{"adcode":330104,"city":"?????????","city_code":179,"country":"??????","country_code":0,"direction":"","distance":"","district":"?????????","province":"?????????","street":"?????????","street_number":""},"business":"","poi_desc":"????????????(??????)???????????????65???","poi_region":[],"point":{"x":120.34921887098,"y":30.284539279398},"surround_poi":[{"addr":"???????????????????????????","cp":" ","direction":"???","distance":"65","name":"????????????(??????)????????????","poiType":"????????????","point":{"x":120.349
    //    如果一个CONTENT中仅包含point一个经纬度，则取point
    //    需要注意，一个CONTENT字段中可能包含多个该POINT经纬度，取第一个，后面的DROP
    //    单独的point多为POI检索，可信度一般
    //    百度坐标
    //4. api.map.baidu.com
    //    "location":{                "lat":30.331446,                "lng":120.347053            },
    //    --{    "status":0,    "message":"ok",    "total":0,    "results":[        {            "name":"?????????558???",            "location":{                "lat":30.331145,                "lng":120.345378            },            "address":"?????????"        },        {            "name":"?????????518",            "location":{                "lat":30.331446,                "lng":120.347053            },            "address":"?????????"        },        {            "name":"?????????768???",            "location":{                "lat":30.329564,                "lng":120.3365
    //    为一些列地址对应的位置点（地图检索周边小吃等等类似场景），可信度低
    //    取第一个，后面的DROP
    //    百度坐标
    else if (lowerCaseHost.equals("api.map.baidu.com")) {
      val indexLng = lowerCaseHttp_Content.indexOf("\"lng\"")
      val indexLat = lowerCaseHttp_Content.indexOf("\"lat\"")
      if (lowerCaseHttp_Content.indexOf("\"location\"") != -1 && indexLng != -1 && indexLat != -1) {
        var splitstr: String = "\\,|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "BD"
        success = true
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"lng\":")) {
            longitude = tempItem.replace("\"lng\":", "").trim()
          } else if (tempItem.startsWith("\"lat\":")) {
            latitude = tempItem.replace("\"lat\":", "").trim()
          } else if (tempItem.startsWith("\"confidence\":")) {
            radius = tempItem.replace("\"confidence\":", "").trim()
          }
        }
      }
    }
    //5. loc.map.baidu.com
    //    rd.go.10086.cn
    //    "point":{"x":"120.268353","y":"30.375310"},"radius":"66.474369"},
    //    --{"content":{"bldg":"","clf":"120.275640(30.365383(2000.000000","floor":"","indoor":"0","point":{"x":"120.268353","y":"30.375310"},"radius":"66.474369"},"result":{"error":"161","time":"2016-10-20 19:43:33"}}
    //    CLF坐标放弃，取POINT坐标及radius字段。
    //    百度坐标
    else if (lowerCaseHost.equals("loc.map.baidu.com") || lowerCaseHost.equals("rd.go.10086.cn")) {
      val indexLng = lowerCaseHttp_Content.indexOf("\"x\"")
      val indexLat = lowerCaseHttp_Content.indexOf("\"y\"")
      if (lowerCaseHttp_Content.indexOf("\"point\"") != -1 && indexLng != -1 && indexLat != -1) {
        var splitstr: String = "\\,|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "BD"
        success = true
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"x\":")) {
            longitude = tempItem.replace("\"x\":", "").trim()
          } else if (tempItem.startsWith("\"y\":")) {
            latitude = tempItem.replace("\"y\":", "").trim()
          } else if (tempItem.startsWith("\"radius\":")) {
            radius = tempItem.replace("\"radius\":", "").trim()
          }
        }

        // CLF坐标放弃，取POINT坐标及radius字段。
        // 仅针对该规则中，结果中增加一个定位方式字段，在符合本规则中，寻找“wf”、“cl”、“ll”特征（注：其中“cl”不能为“clf”）
        // 记录三个值：wf、cl、ll
        // Schema for type java.util.UUID is not supported
        if (lowerCaseHttp_Content.indexOf("\"wf\"") != -1)
          positionType = "wf"
        else if (lowerCaseHttp_Content.indexOf("\"cl\"") != -1)
          positionType = "cl"
        else if (lowerCaseHttp_Content.indexOf("\"ll\"") != -1)
          positionType = "ll"
      }
    }
    //6. ******m5.amap.com******
    //    {"distance": "68.4773", "direction": "North", "name": "\u6e56\u5885\u5357\u8def--\u738b\u5b50\u8857", "weight": "130", "level": "44000, 45000", "longitude": "120.1555667", "crossid": "0571H51F0210021101--0571H51F021002340197", "width": "16, 8", "latitude": "30.27709667"}
    //    --{"province": "\u6d59\u6c5f\u7701", "cross_list": [{"distance": "68.4773", "direction": "North", "name": "\u6e56\u5885\u5357\u8def--\u738b\u5b50\u8857", "weight": "130", "level": "44000, 45000", "longitude": "120.1555667", "crossid": "0571H51F0210021101--0571H51F021002340197", "width": "16, 8", "latitude": "30.27709667"}, {"distance": "133.997", "direction": "SouthEast", "name": "\u6e56\u5885\u5357\u8def--\u6587\u6656\u8def", "weight": "140", "level": "44000, 44000", "longitude": "120.1548272", "crossid": "0571H51F0210021101--0571H51F021002688", "width": "16, 28", "latitude": "30.2787175"}, {"d
    //    地址经纬度，可信度较低
    //    火星坐标
    //8. ******m5.amap.com******
    //    "view_region": "120.193464558,30.216298163,120.202591442,30.202607837",
    //    --{"bus_list": [], "codepoint": 0, "code": "1", "suggestion": {}, "busline_count": "0", "timestamp": "1476963849.32", "lqii": {"suggestionview": "1", "cache_directive": {"cache_all": {"flag": "0", "expires": "24"}}, "utd_sceneid": "101000", "call_taxi": "0", "car_icon_flag": "0", "is_current_city": "1", "slayer": "0", "querytype": "5", "slayer_type": "none", "specialclassify": "0", "view_region": "120.193464558,30.216298163,120.202591442,30.202607837", "suggest_query": {"data": [], "col": "", "row": ""}, "render_name_flag": "1", "is_view_city": "1", "is_tupu_sug": "0"}, "is_general_search": "0",
    //    地图可视范围，经纬度不可信，放弃。
    //9. restapi.amap.com
    //    "origin":"120.162699,30.134971",
    //    --{"status":"1","info":"ok","infocode":"10000","count":"1","route":{"origin":"120.162699,30.134971","destination":"120.160917,30.137208","paths":[{"distance":"294","duration":"210","steps":[{"instruction":"??????????????????????????????294??????????????????","orientation":"??????","road":"????????????","distance":"294","duration":"210","polyline":"120.162727,30.134983;120.162575,30.1353;120.162399,30.135603;120.162178,30.135933;120.16201,30.136141;120.161911,30.136242;120.161842,30.136311;120.161789,30.136362;120.161568,30.136555;120.161308,30.136749;120.160889,30.13707","action":[],"assistant_a
    //    导航开始及目的位置，和中间折现点位置。取开始位置。
    //    火星坐标
    else if (lowerCaseHost.equals("restapi.amap.com")) {
      val indexOrigin = lowerCaseHttp_Content.indexOf("\"origin\"")
      if (indexOrigin != -1) {
        var splitstr: String = "\\\"\\,\\\"|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "GCJ02"
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"origin\":")) {
            var lngLatItems = tempItem.replace("\"origin\":", "").trim().split("\\,")
            if (lngLatItems.length == 2) {
              success = true
              longitude = lngLatItems(0)
              latitude = lngLatItems(1)
            }
          } else if (tempItem.startsWith("\"radius\":")) {
            radius = tempItem.replace("\"radius\":", "")
          }
        }
      }
    }
    //10. ******restapi.amap.com******
    //    "location":"120.134568,30.1772719",
    //    --{"status":"1","info":"OK","infocode":"10000","regeocode":{"formatted_address":"????????????????????????????????????????????????1669???","addressComponent":{"country":"??????","province":"?????????","city":"?????????","citycode":"0571","district":"?????????","adcode":"330108","township":"????????????","towncode":"330108003000","neighborhood":{"name":[],"type":[]},"building":{"name":[],"type":[]},"streetNumber":{"street":"?????????","number":"1669???","location":"120.134568,30.1772719","direction":"???","distance":"19.4482"},"businessAreas":[{"location":"120.15294029081633,30.164365186224465","n
    //    查询地址对应位置坐标，可信度低，放弃
    //11. route.map.baidu.com
    //    "location":{            "lng":120.16870999682,            "lat":30.179330126914        },
    //    --{    "status":0,    "result":{        "location":{            "lng":120.16870999682,            "lat":30.179330126914        },        "formatted_address":"????????????????????????????????????470",        "business":"?????????,??????",        "addressComponent":{            "adcode":330108,            "city":"?????????",            "country":"??????",            "country_code":0,            "direction":"???",            "distance":"97",            "district":"?????????",            "province":"?????????",            "street":"?????????",            "street_number":"470"
    //    百度坐标
    else if (lowerCaseHost.equals("route.map.baidu.com")) {
      val indexLng = lowerCaseHttp_Content.indexOf("\"lng\"")
      val indexLat = lowerCaseHttp_Content.indexOf("\"lat\"")
      if (lowerCaseHttp_Content.indexOf("\"location\"") != -1 && indexLng != -1 && indexLat != -1) {
        var splitstr: String = "\\,|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "BD"
        success = true
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"lng\":")) {
            longitude = tempItem.replace("\"lng\":", "").trim()
          } else if (tempItem.startsWith("\"lat\":")) {
            latitude = tempItem.replace("\"lat\":", "").trim()
          } else if (tempItem.startsWith("\"confidence\":")) {
            radius = tempItem.replace("\"confidence\":", "").trim()
          }
        }
      }
    }
    //12. ******route.map.baidu.com******
    //    "display":{                "lat":30.261124,                "lng":120.168691            },
    //    --{    "status":0,    "total":3,    "results":[        {            "uid":"1008c832eaaa556ca2d23045",            "name":"??????????????????-???????????????",            "addr":"??????????????????124???",            "street_id":"1008c832eaaa556ca2d23045",            "display":{                "lat":30.261124,                "lng":120.168691            },            "areaid":2835,            "dis":192,            "price":"5???/??????",            "total_num":287,            "left_num":76        },        {            "uid":"524b138d3fb77fbac056ac09",            "name":"???????
    //    查询地址对应位置坐标，可信度低，放弃
    //******13. sns.amap.com******
    //    "fence_center":"120.15991,30.25411"
    //    --{"fencing_event_list":[{"fence_info":{"fence_center":"120.15991,30.25411","fence_gid":"567fcb37-2e90-4ea9-9cb4-cee52f160d35","fence_name":"??????????????????0302","is_in_alerttime":"false"}}],"msg":{"code":"1","data":{"next_request_time":1440.0,"status":1},"message":"Successful.","result":"true","timestamp":"1476963832.87","version":"2.0-2.0.6287.1606"},"nearest_fence_distance":"2000.0","status":"0"}
    //    高德地图围栏，可信度取决于围栏大小，可信度一般
    //    火星坐标
    //14. trafficapp.autonavi.com:8888
    //    <lon>120.1768039</lon><lat>30.28316083</lat>
    //    --<?xml version="1.0" encoding="gbk"?> <response type="trafficinfo" msgtype="Incident" detailType="2"> <status>0</status> <timestamp>20161020194415</timestamp> <updatetime>194408</updatetime> <front> <updatetime>194408</updatetime> <description><![CDATA[????2??????????????????????????????????]]></description> <signature nearby="0" dist="-1" class="6"><event><type>201</type><layer>1065</layer><layertag>11040</layertag><id>114886501</id><lon>120.1768039</lon><lat>30.28316083</lat><sourcedesc>????????????????</sourcedesc><brief>????????????</brief></event></signature> </front> </response>
    //    火星坐标
    else if (lowerCaseHost.equals("trafficapp.autonavi.com:8888")) {
      val indexCenxBegin = lowerCaseHttp_Content.indexOf("<lon>")
      val indexCenxEnd = lowerCaseHttp_Content.indexOf("</lon>")
      val indexCenyBegin = lowerCaseHttp_Content.indexOf("<lat>")
      val indexCenyEnd = lowerCaseHttp_Content.indexOf("</lat>")
      val indexRadiusBegin = lowerCaseHttp_Content.indexOf("<radius>")
      val indexRadiusEnd = lowerCaseHttp_Content.indexOf("</radius>")

      if (indexCenxBegin != -1 && indexCenxEnd != -1
        && indexCenyBegin != -1 && indexCenyEnd != -1) {
        success = true
        longitude = lowerCaseHttp_Content.substring(indexCenxBegin + "<lon>".length, indexCenxEnd).trim()
        latitude = lowerCaseHttp_Content.substring(indexCenyBegin + "<lat>".length, indexCenyEnd).trim()
        if (indexRadiusBegin != -1 && indexRadiusEnd != -1) {
          radius = lowerCaseHttp_Content.substring(indexRadiusBegin + "<radius>".length, indexRadiusEnd).trim()
        }
        lnglatType = "GCJ02"
      }
    }

    longitude = longitude.replace("\"", "")
    latitude = latitude.replace("\"", "")
    radius = radius.replace("\"", "")

    (success, lnglatType, longitude, latitude, radius, positionType)
  }

  def parse_uri(uri: String): Array[(String, String)] = {
    var latitude: String = null
    var longitude: String = null
    if (uri == null) {
      return Array((latitude, longitude))
    }

    if (parseXYWithEqualsChar(uri, "s_y=", "s_x=").head._1) {
      parseXYWithEqualsChar(uri, "s_y=", "s_x=").head._2
    }
    else if (parseXY(uri, "x=", "y=").head._1) {
      parseXY(uri, "x=", "y=").head._2
    }
    else if (parseXYWithEqualsChar(uri, "lng=", "lat=").head._1) {
      parseXYWithEqualsChar(uri, "lng=", "lat=").head._2
    }
    //http://common.diditaxi.com.cn/passenger/getredpoint?_t=1479167539&appVersion=4.3.12&appversion=4.3.12&channel=102&clientType=1&datatype=101&imei=8787db686f34ec8ae0aec68899e1bdf2
    // &imsi=&lat=30.31243923611111&lng=120.2174****49653&maptype=soso&mobileType=iPhone&model=iPhone&networkType=UNKOWN&os=10.0.2&osType=1&osVersion=10.0.2&sig=46fe82b23734c651b6d349f6d1f3376004301856&timestamp=1479167539259&token=KdHuAAgfNTlnIw_OKVnM74OU-tR2gpoERfEhgzoTIo1UjLsOQjEMQ__Fc4Y8Sprmb3jDgJComK7674TxbraPdTYckQDhhNSQ1tsI82belXBByi
    else if (parseXY(uri, "lng=", "lat=").head._1) {
      parseXY(uri, "lng=", "lat=").head._2
    }
    else if (parseXY(uri, "lng%3d", "lat%3d").head._1) {
      parseXY(uri, "lng%3d", "lat%3d").head._2
    }
    else if (parseXY(uri, "lng%3A", "lat%3A").head._1) {
      parseXY(uri, "lng%3A", "lat%3A").head._2
    }
    else if (parseXY(uri, "lng%22%3a%22", "lat%22%3a%22").head._1) {
      parseXY(uri, "lng%22%3a%22", "lat%22%3a%22").head._2
    }
    else if (parseXY(uri, "lon=", "lat=").head._1) {
      parseXY(uri, "lon=", "lat=").head._2
    }
    else if (parseXY(uri, "lon%3D", "lat%3D").head._1) {
      parseXY(uri, "lon%3D", "lat%3D").head._2
    }
    else if (parseXY(uri, "don=", "lat=").head._1) {
      parseXY(uri, "don=", "lat=").head._2
    }
    else if (parseXY(uri, "lgt=", "lat=").head._1) {
      parseXY(uri, "lgt=", "lat=").head._2
    }
    else if (parseXY(uri, "longitude%22%3a%22", "latitude%22%3a%22").head._1) {
      parseXY(uri, "longitude%22%3a%22", "latitude%22%3a%22").head._2
    }
    else if (parseXY(uri, "longitude=", "latitude=").head._1) {
      parseXY(uri, "longitude=", "latitude=").head._2
    }
    else if (parseXY(uri, "long=", "lat=").head._1) {
      parseXY(uri, "long=", "lat=").head._2
    }
    else if (parseXY(uri, "MyPosx=", "MyPosy=").head._1) {
      parseXY(uri, "MyPosx=", "MyPosy=").head._2
    }
    else if (parseXY(uri, "pointx=", "pointy=").head._1) {
      parseXY(uri, "pointx=", "pointy=").head._2
    }
    else if (parseXY(uri, "lou%22%20:%20%22", "lau%22%20:%20%22").head._1) {
      parseXY(uri, "lou%22%20:%20%22", "lau%22%20:%20%22").head._2
    }
    else if (parseXY(uri, "logi=", "lati=").head._1) {
      parseXY(uri, "logi=", "lati=").head._2
    }
    else if (parseXYWithEqualsChar(uri, "m=", 0, 1).head._1) {
      parseXYWithEqualsChar(uri, "m=", 0, 1).head._2
    }
    else if (parseXY4(uri, "xy%22%3A%22", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%22%7d%5d|%22%7D%5D|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 0, 1)._1) {
      parseXY4(uri, "xy%22%3A%22", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%22%7d%5d|%22%7D%5D|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 0, 1)._2.toArray
    }
    else if (parseXY2(uri, "action_gps=")._1) {
      parseXY2(uri, "action_gps=")._2.toArray
    }
    else if (parseXY2(uri, "cur_pt=")._1) {
      parseXY2(uri, "cur_pt=")._2.toArray
    }
    else if (parseXY2(uri, "center=")._1) {
      parseXY2(uri, "center=")._2.toArray
    }
    else if (parseXY2(uri, "cll=")._1) {
      parseXY2(uri, "cll=")._2.toArray
    }
    else if (parseXY2(uri, "geoinfo=")._1) {
      parseXY2(uri, "geoinfo=")._2.toArray
    }
    else if (parseXY2(uri, "gps=", 1, 0)._1) {
      parseXY2(uri, "gps=", 1, 0)._2.toArray
    }
    else if (parseXY2(uri, "loc=")._1) {
      parseXY2(uri, "loc=")._2.toArray
    }
    else if (parseXY2(uri, "location=", 1, 0, "http://restapi.amap.com")._1) {
      parseXY2(uri, "location=", 1, 0, "http://restapi.amap.com")._2.toArray
    }
    else if (parseXY2(uri, "location=")._1) {
      parseXY2(uri, "location=")._2.toArray
    }
    else if (parseXY2(uri, "location=", 1, 0)._1) {
      parseXY2(uri, "location=", 1, 0)._2.toArray
    }
    else if (parseXY2(uri, "latlng=")._1) {
      parseXY2(uri, "latlng=")._2.toArray
    }
    else if (parseXY2(uri, "mypos=")._1) {
      parseXY2(uri, "mypos=")._2.toArray
    }
    else if (parseXY2(uri, "origin=")._1) {
      parseXY2(uri, "origin=")._2.toArray
    }
    else if (parseXY2(uri, "points=")._1) {
      parseXY2(uri, "points=")._2.toArray
    }
    else if (parseXY2(uri, "point=")._1) {
      parseXY2(uri, "point=")._2.toArray
    }
    else if (parseXY2(uri, "position=")._1) {
      parseXY2(uri, "position=")._2.toArray
    }
    else if (parseXY2(uri, "q=")._1) {
      parseXY2(uri, "q=")._2.toArray
    }
    else if (parseXY2(uri, "xyr=")._1) {
      parseXY2(uri, "xyr=")._2.toArray
    }
    else if (parseXY4(uri, "coords:", "%2C|%2c|,", "_", 0, 1)._1) {
      parseXY4(uri, "coords:", "%2C|%2c|,", "_", 0, 1)._2.toArray
    }
    else if (parseXY4(uri, "coords%3D", "%2C|%2c|,", "_", 0, 1)._1) {
      parseXY4(uri, "coords%3D", "%2C|%2c|,", "_", 0, 1)._2.toArray
    }
    else if (parseXY4(uri, "click=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._1) {
      parseXY4(uri, "click=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._2.toArray
    }
    else if (parseXY4(uri, "start=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._1) {
      parseXY4(uri, "start=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._2.toArray
    }
    else if (parseXY4(uri, "JW%3A", "%2C|%2c,", "_|\\|", 1, 0) _1) {
      parseXY4(uri, "JW%3A", "%2C|%2c", ",_|\\|", 1, 0)._2.toArray
    }
    else if (parseXY4(uri, "JW:", "%2C|%2c|,", "_|\\|", 1, 0)._1) {
      parseXY4(uri, "JW:", "%2C|%2c|,", "_|\\|", 1, 0)._2.toArray
    }
    else if (parseXY3(uri, "ps.map.baidu.com", "sessid=", "&", "_|\\|", "%2C|%2c|,", 0, 1)._1) {
      parseXY3(uri, "ps.map.baidu.com", "sessid=", "&", "_|\\|", "%2C|%2c|,", 0, 1)._2.toArray
    }
    else if (parseXY(uri, "lt%22%3A%22", "ltt%22%3A%22").head._1) {
      parseXY(uri, "lt%22%3A%22", "ltt%22%3A%22").head._2
    }
    else if (parseXY(uri, "lo=", "la=").head._1) {
      parseXY(uri, "lo=", "la=").head._2
    }
    else if (parseXY(uri, "d=", "l=").head._1) {
      parseXY(uri, "d=", "l=").head._2
    }
    else if (parseXY(uri, "l=", "x=").head._1) {
      parseXY(uri, "l=", "x=").head._2
    }
    else if (parseXY(uri, "px=", "py=").head._1) {
      parseXY(uri, "px=", "py=").head._2
    }
    else {
      breakable {
        if (uri.indexOf("-lat") != -1 && uri.indexOf("-lng") != -1) {
          val uriArr: Array[String] = uri.split("-")
          for (uriPartial: String <- uriArr) {
            if (uriPartial.indexOf("lat") != -1) {
              latitude = uriPartial.replace("lat", "")
              if (longitude != null) {
                break
              }
            }
            if (uriPartial.indexOf("lng") != -1) {
              longitude = uriPartial.replace("lng", "")
              if (latitude != null) {
                break
              }
            }
          }
        }
      }

      Array((latitude, longitude))
    }
  }

  /// <summary>
  /// &from_lat=%28null%29&from_lng=%28null%29&...&lat=30.325195312500&lng=120.099913465712& http://common.diditaxi.com.cn/poiservice/addrrecommend?_t=1472265437&acckey=T7JNA-HRGLG-4N2KY-XX8QE-0RDGW-122J3&appVersion=4.4.4&appversion=4.4.4&channel=102&clientType=1&datatype=101&debugKey=1472265437440%2B02328198adf58916beb31f1fd9acd5a9&departure_time=1472265437&from_lat=%28null%29&from_lng=%28null%29&imei=02328198adf58916beb31f1fd9acd5a9&imsi=&lat=30.325195312500&lng=120.099913465712&maptype=soso&mobileType=iPhone%206%20Plus&model=iPhone&networkType=4G&os=9.3.4&osType=1&osVersion=9.3.4&passengerid=283
  /// </summary>
  /// <returns></returns>
  def parseXYWithEqualsChar(uri: String, lngEqualsChar: String, latEqualsChar: String): Map[Boolean, Array[(String, String)]] = {
    var lng: String = null
    var lat: String = null

    if (uri.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) == -1 | uri.toLowerCase().indexOf(latEqualsChar.toLowerCase()) == -1) {
      Map(false -> Array((lat, lng)))
    }
    else {
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.indexOf(lngEqualsChar.toLowerCase()) != -1) {
            lng = uriItem.substring(uriItemLowerCase.indexOf(lngEqualsChar.toLowerCase()) + lngEqualsChar.length)
            if (lat != null)
              break
          }
          if (uriItemLowerCase.indexOf(latEqualsChar.toLowerCase()) != -1) {
            lat = uriItemLowerCase.substring(uriItem.indexOf(latEqualsChar.toLowerCase()) + latEqualsChar.length)
            if (lng != null)
              break
          }
        }
      }
      Map((lat != null && lng != null) -> Array((lat, lng)))
    }
  }

  def parseXYWithEqualsChar(uri: String, lngEqualsChar: String, latIndex: Int, lngIndex: Int): Map[Boolean, Array[(String, String)]] = {
    var lng: String = null
    var lat: String = null

    if (uri.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) == -1) {
      Map(false -> Array((lat, lng)))
    }
    else {
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.startsWith(lngEqualsChar.toLowerCase())) {
            var tempValue: String = uriItemLowerCase.substring(uriItem.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) + lngEqualsChar.length)
            var tempArray: Array[String] = uri.split("%2C|%2c|,")
            if (tempArray != null && tempArray.length >= 2) {
              lat = tempArray(latIndex) //tempArray[latIndex]
              lng = tempArray(lngIndex)

              if (lng.indexOf("(") != -1) {
                lng = lng.substring(0, lng.indexOf("("))
              }
            }
            break
          }
        }
      }
      Map((lat != null && lng != null) -> Array((lat, lng)))
    }
  }

  def parseXY(uri: String, lngChar: String, latChar: String): Map[Boolean, Array[(String, String)]] = {
    var lng: String = null
    var lat: String = null

    // &imsi=&lat=30.31243923611111&lng=120.2174****49653&
    if (uri.toLowerCase().indexOf(lngChar.toLowerCase()) == -1 | uri.toLowerCase().indexOf(latChar.toLowerCase()) == -1) {
      Map(false -> Array((lat, lng)))
    }
    else {
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.indexOf(lngChar.toLowerCase()) != -1) {
            lng = uriItem.substring(uriItemLowerCase.indexOf(lngChar.toLowerCase()) + lngChar.length)
            if (lat != null)
              break
          }
          if (uriItemLowerCase.indexOf(latChar.toLowerCase()) != -1) {
            lat = uriItem.substring(uriItemLowerCase.indexOf(latChar.toLowerCase()) + latChar.length)
            if (lng != null)
              break
          }
        }
      }
      Map((lat != null && lng != null) -> Array((lat, lng)))
    }
  }

  def parseXY2(uri: String, lngLatChar: String, latIndex: Int = 0, lngIndex: Int = 1, specialHost: String = ""): Tuple2[Boolean, List[Tuple2[String, String]]] = {
    var lng: String = null
    var lat: String = null

    if (specialHost != "" && uri.toLowerCase().indexOf(specialHost.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else if (uri.toLowerCase().indexOf(lngLatChar.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else {
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.indexOf(lngLatChar.toLowerCase()) != -1) {
            var tempValue: String = uriItem.substring(uriItemLowerCase.indexOf(lngLatChar.toLowerCase()) + lngLatChar.length)
            var tempArray: Array[String] = uri.split("%2C|%2c|,")
            if (tempArray != null && tempArray.length == 2) {
              lat = tempArray(latIndex) //tempArray[latIndex]
              lng = tempArray(lngIndex)

              if (lng.indexOf("(") != -1) {
                lng = lng.substring(0, lng.indexOf("("));
              }
            }
            break
          }
        }
      }
      Tuple2(lng != null && lat != null, List(Tuple2(lat, lng)))
    }
  }

  def parseXY3(uri: String, host: String, lngLatChar: String, splitChars: String, beginEndChars: String, lngLatSplitChars: String, latIndex: Int = 0, lngIndex: Int = 0): Tuple2[Boolean, List[Tuple2[String, String]]] = {
    var lng: String = null
    var lat: String = null
    var items: List[Tuple2[String, String]] = List()

    if (uri.toLowerCase().indexOf(host.toLowerCase()) == -1 | uri.toLowerCase().indexOf(lngLatChar.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else {
      var uriItems: Array[String] = uri.split(splitChars)
      breakable {
        for (uriItem: String <- uriItems) {
          if (uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) != -1) {
            var tempValue: String = uriItem.substring(uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) + lngLatChar.length)
            var tempArray: Array[String] = tempValue.split(beginEndChars)
            for (tempItem: String <- tempArray) {
              var tempItemItems: Array[String] = tempItem.split(lngLatSplitChars)
              if (tempItemItems != null && tempItemItems.length == 2) {
                lat = tempItemItems(latIndex)
                lng = tempItemItems(lngIndex)

                if (lng.indexOf("(") != -1) {
                  lng = lng.substring(0, lng.indexOf("("))
                }

                if (lat != null && lng != null && Try(lat.replace("%", "").toDouble > 0.0).isSuccess && Try(lng.replace("%", "").toDouble > 0.0).isSuccess) {
                  items = List.concat(items, List(Tuple2(lat.replace("%", ""), lng.replace("%", ""))))
                }
              }
            }
            break
          }
        }
      }
      Tuple2(items.size != 0, items)
    }
  }

  def parseXY4(uri: String, lngLatChar: String, splitChars: String, lngLatSplitChars: String, latIndex: Int = 0, lngIndex: Int = 0): Tuple2[Boolean, List[Tuple2[String, String]]] = {
    var lng: String = null
    var lat: String = null
    var items: List[Tuple2[String, String]] = List()

    if (uri.toLowerCase().indexOf(lngLatChar.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else {
      var uriItems: Array[String] = uri.split(splitChars)
      for (uriItem: String <- uriItems) {
        if (uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) != -1) {
          var tempValue: String = uriItem.substring(uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) + lngLatChar.length);
          var tempArray: Array[String] = tempValue.split(lngLatSplitChars)

          if (tempArray != null && tempArray.length == 2) {
            lat = tempArray(latIndex)
            lng = tempArray(lngIndex)

            if (lng.indexOf("(") != -1) {
              lng = lng.substring(0, lng.indexOf("("));
            }

            if (lat != null && lng != null && Try(lat.replace("%", "").toDouble > 0.0).isSuccess && Try(lng.replace("%", "").toDouble > 0.0).isSuccess) {
              items = List.concat(items, List(Tuple2(lat.replace("%", ""), lng.replace("%", ""))))
            }
          }
        }
      }

      Tuple2(lng != null && lat != null, items)
    }
  }
}
