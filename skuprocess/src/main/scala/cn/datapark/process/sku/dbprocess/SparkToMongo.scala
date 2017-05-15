package cn.datapark.process.sku.dbprocess

import java.text.{SimpleDateFormat, DateFormat}
import java.util
import java.util.Date

import com.mongodb.MongoClient
import com.mongodb.client.model.Filters
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.bson.Document
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer

/** 这个 spark 写 mongo程序 是  kafka-->spark--->mongo
  * 第一版
  * Created by cluster on 2017/3/1.
  */
object SparkToMongo {

  // *****获取Mongo的连接*****
  //通过连接认证获取MongoDB连接
  val mongoClient: MongoClient  = new MongoClient("192.168.39.96", 27017)
  //连接到 数据库
  val mongoDatabase: MongoDatabase  = mongoClient.getDatabase("SKU_test")
  println("Connect to database successfully")
  //获取集合 参数为“集合名称”
  val mongoCollection: MongoCollection[Document]  = mongoDatabase.getCollection("sku_change_test")
  println("Connect to Collection successfully")
  //存放src_url,以便以后判断数据是否存在
  val set_skuid = new util.HashSet[String]()
  // 存放新生成的document
  var doc_update: Document = null
  // 存放从mongo中获取的bson
  var doc_moc: Document = null



  def toMongo(jsonSku: JSONObject): Unit ={

    if (isSKUExist(jsonSku.getJSONObject("id").getString("sku_id").trim)){// ***添加判断sku是否存在
      //      ***如果存在，则替换对应字段值
      updateExistSKU(jsonSku)
      return
    }else {// ***如果不存在，则直接存入MongoDB中
      val doc: Document = new Document() //创建文档对象
      if (!jsonSku.has("color_alias")){   //如果没有 color_alias 则不存
        return
      }
      val colorArray = jsonSku.get("color_alias").toString.replace("[", "").replace("]", "").split(",")
      val colorlist: util.ArrayList[String] = new util.ArrayList[String]()
      for (i <- 0 to colorArray.length - 1){
        colorlist.add(i,colorArray(i))
      }
      colorlist  //转成list存color，存数组出问题

      val format: DateFormat = new SimpleDateFormat("yyyyMMdd")//将时间转换成这种格式
      val seentime: String = format.format(new Date(jsonSku.getString("shelvetime")))

      //      increment的计算方法
      //      "increment":Math.abs(Integer.parseInt(今天的sales值) - ( 昨天的sales值 - 昨天的sales值 / 30))
      //如果是新数据，首次存入，则增量设为 0
      val increment = 0
      val list: util.ArrayList[Int] = new util.ArrayList[Int]()
      list.add(0,jsonSku.getInt("sales"))
      list.add(1,increment)
      //判断 discountprice 数据类型
      val discount = jsonSku.get("discountprice")
      if (discount.isInstanceOf[Double]) {
        val discount = jsonSku.getDouble("discountprice")
      } else if (discount.isInstanceOf[Integer]) {
        val discount = jsonSku.getInt("discountprice")
      }

      val formatlasttime: DateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
      //向文档对象中插入新数据，存入Mongo
      doc.append("_id",jsonSku.getJSONObject("id").getString("sku_id").trim)
      doc.append("item_id",jsonSku.getJSONObject("id").getString("item_id").trim)
      doc.append("brand_productid",jsonSku.getJSONObject("id").getString("brand_productid").trim)
      doc.append("color",colorlist)
      //add   sales,storeup,stock,commentcount,discountprice,lastseentime
      doc.append("sales",jsonSku.getInt("sales"))
      doc.append("storeup",jsonSku.getInt("storeup"))
      doc.append("stock",jsonSku.getInt("stock"))
      doc.append("commentcount",jsonSku.getInt("commentcount"))
      doc.append("discountprice",discount)
      doc.append("lastseentime",formatlasttime.format(new Date(jsonSku.getString("lastseentime")))) //格式2017/02/24 20:21:33
      //end
      doc.append("storeup_list", new Document().append(seentime,jsonSku.getInt("storeup")))
      doc.append("saleslist",new Document().append(seentime,list))
      doc.append("discountlist",new Document().append(seentime,discount))
      doc.append("stocklist",new Document().append(seentime,jsonSku.getInt("stock")))
      doc.append("commentcountlist",new Document().append(seentime,jsonSku.getInt("commentcount")))
      doc.append("otherinfo",new Document())
      mongoCollection.insertOne(doc)
      //添加otherInfo 选择在后面更新的方式
      addOtherInfo(jsonSku)
      //      mongoCollection.insertMany(docList)
    }
  }

  def addOtherInfo(jsonSku: JSONObject): Unit ={
    val doc_other: Document = new Document() //创建文档对象
    val docList: util.List[Document] = new util.ArrayList[Document]()
    val otherinfo = jsonSku.get("otherinfo").asInstanceOf[JSONObject]
    val iter: util.Iterator[String] = otherinfo.keys()
//    val doc_other: Document = new Document() //创建文档对象
    var key: String = ""
    var value = ""
      while (iter.hasNext){
      key = iter.next()
      value = otherinfo.get(key).toString
      mongoCollection.updateOne(Filters.eq("_id", jsonSku.getJSONObject("id").getString("sku_id").trim),new Document("$set", new Document("otherinfo", doc_other.append(key,value))))
    }
  }
  /**
    * 判断要存入的json数据是否存在
    *
    * @param sku_id json数据中的 sku_id
    * @return 返回布尔类型数据
    */
  def isSKUExist(sku_id: String): Boolean={
    (mongoCollection.find(Filters.eq("_id",sku_id))).first() != null
  }

  /**
    * 更新Mongo中已经存在的数据字段 discountlist,sales,stock,storeup,comment
    *
    * @param jsonSku 获取的json数据
    */
  def updateExistSKU(jsonSku: JSONObject): Unit ={
    //获取存在的document
    val moc = mongoCollection.find(Filters.eq("_id",jsonSku.getJSONObject("id").getString("sku_id").trim)).first()
    val format: DateFormat = new SimpleDateFormat("yyyyMMdd")//将时间转换成这种格式
    val seentime: String = format.format(new Date(jsonSku.getString("shelvetime")))
    //sales,storeup,stock,commentcount,discountprice,lastseentime
    //***** 判断更新storeup ， 向storeup_list 中添加新值
    if (jsonSku.getInt("storeup") != moc.get("storeup").asInstanceOf[Int]){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("storeup", jsonSku.getInt("storeup"))))
    }
    doc_moc = moc.get("storeup_list").asInstanceOf[Document]
    if(!doc_moc.containsKey(seentime)){
      doc_update = doc_moc.append(seentime,jsonSku.getInt("storeup"))
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("storeup_list", doc_update)))
    }

    //***** 向discountlist 中添加值
    if (jsonSku.get("discountprice") != moc.get("discountprice")){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountprice", jsonSku.get("discountprice"))))
    }
    if (jsonSku.get("discountprice").isInstanceOf[Double]) {
      val discountprice = jsonSku.getDouble("discountprice")
      doc_moc = moc.get("storeup_list").asInstanceOf[Document]
      if(!doc_moc.containsKey(seentime)){
        doc_update = doc_moc.append(seentime,discountprice)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountlist", doc_update)))
      }
    } else if (jsonSku.get("discountprice").isInstanceOf[Integer]) {
      val discountprice = jsonSku.getInt("discountprice")
      doc_moc = moc.get("discountlist").asInstanceOf[Document]
      if(!doc_moc.containsKey(seentime)){
        doc_update = doc_moc.append(seentime,discountprice)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountlist", doc_update)))
      }
    }

    //***** 向 stocklist 中添加值
    if (jsonSku.getInt("stock") != moc.get("stock").asInstanceOf[Int]){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("stock", jsonSku.getInt("stock"))))
    }
    doc_moc = moc.get("stocklist").asInstanceOf[Document]
    if(!doc_moc.containsKey(seentime)){
      doc_update = doc_moc.append(seentime,jsonSku.getInt("stock"))
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("stocklist", doc_update)))
    }

    //*** 向 commentcountlist 中添加值
    if (jsonSku.getInt("commentcount") != moc.get("commentcount").asInstanceOf[Int]){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("commentcount", jsonSku.getInt("commentcount"))))
    }
    doc_moc = moc.get("commentcountlist").asInstanceOf[Document]
    if(!doc_moc.containsKey(seentime)){
      doc_update = doc_moc.append(seentime,jsonSku.getInt("commentcount"))
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("commentcountlist", doc_update)))
    }

    //*** 向 saleslist 中添加值
    if (jsonSku.getInt("sales") != moc.get("sales").asInstanceOf[Int]){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("sales", jsonSku.getInt("sales"))))
    }
    doc_moc = moc.get("saleslist").asInstanceOf[Document]
    if(!doc_moc.containsKey(seentime)){
      val list: util.ArrayList[Int] = new util.ArrayList[Int]()
      //获取上一次的 increment值   来计算现在的increment值
      val saleslistjson: JSONObject = new JSONObject(moc.get("saleslist").asInstanceOf[Document].toJson())  //json类型
      var finalkey = ""
      val iter:util.Iterator[String] = saleslistjson.keys()
      while (iter.hasNext){
        finalkey = iter.next()
      }
      val arrayfinal = moc.get("saleslist").asInstanceOf[Document].get(finalkey).asInstanceOf[util.ArrayList[Int]]
      val increment_new = Math.abs(jsonSku.getInt("sales") - (arrayfinal.get(0)-arrayfinal.get(0) / 30))
      list.add(0,jsonSku.getInt("sales"))
      list.add(1,increment_new)
      doc_update = doc_moc.append(seentime,list)
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("saleslist", doc_update)))
    }

    //*** 更新 lastseentime
    val formatlasttime: DateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
    if (jsonSku.getString("lastseentime") != moc.get("lastseentime").toString){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("lastseentime", formatlasttime.format(new Date(jsonSku.getString("lastseentime"))))))
    }

  }



  def main(args: Array[String]) {
    val jsonstr = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/02/24 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":1}"
    val json1 = new JSONObject(jsonstr)
    toMongo(json1)

//        val jsonstr1 = "{\"src_url\":\"https://detail.tmall.com/item.htm?id=528398333853&skuId=3291298984298&user_id=2172152306&cat_id=50032140&is_b=1&rn=6630d5161c1a0966eed2ce686141e993\",\"color_alias\":[\"灰\",\"白\"],\"othersize\":\"M,L,XL,2XL,3XL,4XL,5XL,8828白色和灰色部份28号发货\",\"othercolor\":\"A8828藏蓝色,A8828白色(M.L.XL码28号发货）,A8828灰蓝色,A8828红色,A8828绿色,A8828白红色,8828藏蓝色+8835白色,8828藏蓝色+8835黑色,8828藏蓝色+8835藏蓝色,8828藏蓝色+8835深灰色,8828藏蓝色+888A黑色,8828藏蓝色+888A白色,8828白色+8835白色(M.L.XL码28号发货）,8828白色+8835黑色(M.L.XL码28号发货）,8828白色+8835藏蓝色(M.L.XL码28号发货）,8828白色+8835深灰色(M.L.XL码28号发货）,8828白色+888A黑色(M.L.XL码28号发货）,8828白色+888A白色(M.L.XL码28号发货）,8828灰色+8835白色,8828灰色+8835黑色,8828灰色+8835藏蓝色,8828灰色+8835深灰色,8828灰色+888A黑色,8828灰色+888A白色\",\"storeup\":66666,\"src_name\":\"天猫\",\"sales\":666,\"discountlist\":[{\"price\":69,\"time\":\"2017/02/24 20:21:33\"}],\"material_alias\":\"棉\",\"style_alias\":\"时尚\",\"stocklist\":[{\"number\":1000,\"time\":\"2017/02/24 20:21:33\"}],\"storeuplist\":[{\"number\":3357,\"time\":\"2017/02/24 20:21:33\"}],\"model\":\" 修身\",\"id\":{\"brand_productid\":\" ST8828\",\"item_id\":\"528398333853\",\"sku_id\":\"3291298984298\"},\"stock\":22,\"brand\":\" 鄂东狼\",\"discountprice\":110,\"pd_src_img_url\":\"//img.alicdn.com/bao/uploaded/i1/2172152306/TB2uwOQe30kpuFjSspdXXX4YXXa_!!2172152306.jpg\",\"commentcountlist\":[{\"number\":2065,\"time\":\"2017/02/24 20:21:33\"}],\"markettime\":\" 2016年春季\",\"print_alias\":\"几何\",\"commentcount\":88,\"collarmodel\":\" 翻领\",\"otherinfo\":{\"厚薄\":\" 常规\",\"基础风格\":\" 时尚都市\",\"适用场景\":\" 日常\",\"面料分类\":\" 其他\",\"袖型\":\" 常规\",\"销售渠道类型\":\" 纯电商(只在线上销售)\",\"适用对象\":\" 青年\",\"袖长\":\" 短袖\",\"细分风格\":\" 潮\",\"款式细节\":\" 拼料\",\"印花主题\":\" 城市风貌\",\"服饰工艺\":\" 免烫处理\"},\"size\":\"8828白色和灰色部份28号发货\",\"name\":\" 夏季男士短袖t恤 翻领条纹polo衫修身韩版潮流半袖体恤男装上衣服 \",\"style\":\" 时尚都市,  潮\",\"sleevemodel\":\" 常规,  短袖\",\"saleslist\":[{\"number\":786,\"increment\":26,\"time\":\"2017/02/24 20:21:33\"}],\"region\":{\"Area\":\"china\",\"city\":\"\"},\"color\":\"8828灰色+888A白色\",\"seentime\":\"2017/02/24 20:21:33\",\"shelvetime\":\"2017/02/26 20:21:33\",\"otherimage\":\"[img.alicdn.com/bao/uploaded/i1/2172152306/TB22kpyppXXXXacXXXXXXXXXXXX_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i3/2172152306/TB2NAo9pXXXXXX6XpXXXXXXXXXX_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2WgFrppXXXXa2XXXXXXXXXXXX_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2LhE2pXXXXXXBXFXXXXXXXXXX_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i2/2172152306/TB2mccWpXXXXXaGXFXXXXXXXXXX_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i4/2172152306/TB2Vl0ippXXXXbYXXXXXXXXXXXX_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i3/2172152306/TB2Fq4_e88lpuFjSspaXXXJKpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i2/2172152306/TB2HuJSe4XkpuFjy0FiXXbUfFXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i4/2172152306/TB2gDWOf90mpuFjSZPiXXbssVXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i3/2172152306/TB2PzGKf0RopuFjSZFtXXcanpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i2/2172152306/TB2zFhaX4RDOuFjSZFzXXcIipXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB24KN2e9xjpuFjSszeXXaeMVXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i3/2172152306/TB2bmbpf4tmpuFjSZFqXXbHFpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2UxvLf.hnpuFjSZFEXXX0PFXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2KffXe80kpuFjy1XaXXaFkVXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2ZtWUe9BjpuFjSsplXXa5MVXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i2/2172152306/TB2_B15e84lpuFjy1zjXXcAKpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i3/2172152306/TB2trS8eYtlpuFjSspoXXbcDpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2.JKUe3NlpuFjy0FfXXX3CpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2R_Lae88lpuFjSspaXXXJKpXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i2/2172152306/TB2Hkq5e3RkpuFjy1zeXXc.6FXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB20wWQe30kpuFjSspdXXX4YXXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i2/2172152306/TB2wjjDf.lnpuFjSZFjXXXTaVXa_!!2172152306.jpg_40x40q90.jpg, img.alicdn.com/bao/uploaded/i1/2172152306/TB2uwOQe30kpuFjSspdXXX4YXXa_!!2172152306.jpg_40x40q90.jpg]\",\"lastseentime\":\"2017/02/24 14:14:14\",\"designer\":{\"name\":\"\",\"desc\":\"\"},\"print\":\" 条纹\",\"material\":\" 棉93.1% 聚氨酯弹性纤维(氨纶)6.9%\",\"shopname\":\"鄂东狼旗舰店\",\"firstseentime\":\"2017/02/24 20:21:33\",\"category\":{\"indextype\":\"men\",\"description\":\"tShirt\",\"style\":\"up\"},\"marketprice\":398}"
//        val json2 = new JSONObject(jsonstr1)
//        toMongo(json2)
  }
}
