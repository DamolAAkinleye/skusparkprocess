package cn.datapark.process.sku.preprocess

import java.util
import java.util.regex.{Matcher, Pattern}

import org.json.{JSONException, JSONObject}

/**
  * Created by Cpeixin on 2016/12/28.
  * Format用于格式化kafka queue中的数据
  * 1. 判断 HBase 中是否存在该sku
  * 2. 如果存在，将本次sku抓取时间，折扣价发送stream中
  * 3. 如果不存在，则为新sku，将sku按照 schema：skuesmapping.json的要求进行格式化，并发送到stream中
  */

object FormatProcess {

  /**
    * 重新获取并替换storeup，sales，commentcount，stock的值
    *
    * @param line
    * @return
    */
  def Formatfield(line: String): JSONObject = {
    val jsonobjScrapedSKU = new JSONObject(line)

    val salesNumStr = getJSONCatchExc(jsonobjScrapedSKU, "sales")
    val stockNumStr = getJSONCatchExc(jsonobjScrapedSKU, "stock")
    val storeupNumStr = getJSONCatchExc(jsonobjScrapedSKU, "storeup")
    val commentcountNumStr = getJSONCatchExc(jsonobjScrapedSKU, "commentcount")

    var salesNum: Int = if (salesNumStr.length == 0) 0 else Integer.valueOf(salesNumStr)
    val stockNum: Int = if (stockNumStr.length == 0) 0 else Integer.valueOf(stockNumStr)
    val storeupNum: Int = if (storeupNumStr.length == 0) 0 else Integer.valueOf(storeupNumStr)
    val commentcountNum: Int = if (commentcountNumStr.length == 0) 0 else Integer.valueOf(commentcountNumStr)
    //此处逻辑为处理 有些网站抓不到销售量，则用评论量替代
    if (salesNum == 0)
      salesNum = commentcountNum

    jsonobjScrapedSKU.put("sales", salesNum)
    jsonobjScrapedSKU.put("stock", stockNum)
    jsonobjScrapedSKU.put("storeup", storeupNum)
    jsonobjScrapedSKU.put("commentcount", commentcountNum)
    //    val stringSeenTime = jsonobjScrapedSKU.getString("seentime")
    jsonobjScrapedSKU
  }

  /**
    * 获取sku字段值
    *
    * @param jobject
    * @param key
    * @return
    */
  def getJSONCatchExc(jobject: JSONObject, key: String): String = {
    var value = ""
    try {
      val pt: Pattern = Pattern.compile("[^0-9]")
      val matcher: Matcher = pt.matcher(jobject.get(key).toString)
      val numValue = matcher.replaceAll("")
      value = numValue
    }
    catch {
      case e: JSONException =>
        value = ""
    }
    value
  }

  /**
    * 格式化sku数据  设置discountlist，storeuplist，saleslist，commentcountlist，stocklist
    *
    * @param jsonSrc
    * @return
    */
  def formatNewSKU(jsonSrc: JSONObject): JSONObject = {
    val seentime: Object = jsonSrc.get("seentime")
    if (seentime.isInstanceOf[String]) {
      if (!seentime.asInstanceOf[String].trim.isEmpty) {
        jsonSrc.put("lastseentime", jsonSrc.getString("seentime"))
        jsonSrc.put("firstseentime", jsonSrc.getString("seentime"))
        //设置折扣价
        //判断折扣价字段是否正确
        val disccountprice: Object = jsonSrc.get("discountprice")
        if (disccountprice.isInstanceOf[Double]) {
          val jsonDiscountlist = new JSONObject
          jsonDiscountlist.put("time", jsonSrc.getString("seentime"))
          jsonDiscountlist.put("price", jsonSrc.getDouble("discountprice"))
          jsonSrc.append("discountlist", jsonDiscountlist)
        } else if (disccountprice.isInstanceOf[Integer]) {
          val jsonDiscountlist: JSONObject = new JSONObject
          jsonDiscountlist.put("time", jsonSrc.getString("seentime"))
          //jsonDiscountlist.put("price", Double.valueOf(jsonSrc.getInt("discountprice")))
          jsonDiscountlist.put("price", jsonSrc.getInt("discountprice").asInstanceOf[Double])
          jsonSrc.append("discountlist", jsonDiscountlist)
        }

        //设置storeup
        val jsonStoreuplist: JSONObject = new JSONObject
        jsonStoreuplist.put("time", jsonSrc.getString("seentime"))
        jsonStoreuplist.put("number", jsonSrc.get("storeup"))
        jsonSrc.append("storeuplist", jsonStoreuplist)

        //设置stock
        val jsonStocklist: JSONObject = new JSONObject
        jsonStocklist.put("time", jsonSrc.getString("seentime"))
        jsonStocklist.put("number", jsonSrc.get("stock"))
        jsonSrc.append("stocklist", jsonStocklist)

        //设置sales
        val jsonSaleslist: JSONObject = new JSONObject
        jsonSaleslist.put("time", jsonSrc.getString("seentime"))
        jsonSaleslist.put("number", jsonSrc.get("sales"))
        //增量sales
        jsonSaleslist.put("increment", jsonSrc.get("sales").asInstanceOf[Integer] / 30)
        jsonSrc.append("saleslist", jsonSaleslist)

        //设置commentcount
        val jsonCommentcountlist: JSONObject = new JSONObject
        jsonCommentcountlist.put("time", jsonSrc.getString("seentime"))
        jsonCommentcountlist.put("number", jsonSrc.get("commentcount"))
        jsonSrc.append("commentcountlist", jsonCommentcountlist)
      }
    }
    //移除seentime
    //jsonSrc.remove("seentime")
    //将json数据中为空和空白内容项去掉
    val it: util.Iterator[String] = jsonSrc.keys
    while (it.hasNext) {
      val key: String = it.next
      val value: AnyRef = jsonSrc.get(key)
      if (value == null) {
        it.remove
      }
      else if (value.isInstanceOf[String]) {
        if ((value.asInstanceOf[String]).trim.equalsIgnoreCase("")) {
          it.remove
        }
      }
    }
    jsonSrc
  }
}
