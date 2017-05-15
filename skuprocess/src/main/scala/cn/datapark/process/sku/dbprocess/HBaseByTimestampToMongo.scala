package cn.datapark.process.sku.dbprocess

import java.text.{SimpleDateFormat, DateFormat}
import java.util
import java.util.Date

import com.mongodb.MongoClient
import com.mongodb.client.model.Filters
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.bson.Document
import org.json.JSONObject

/**
  * Created by cluster on 2017/3/9.
  */
object HBaseByTimestampToMongo {
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

    if (isSKUExist(jsonSku.getString("sku_id"))){// ***添加判断sku是否存在
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
      val list: util.ArrayList[Int] = new util.ArrayList[Int](2)
      if (jsonSku.getInt("sales") != "" && jsonSku.getInt("sales") != null){
        list.add(0,jsonSku.getInt("sales"))
        list.add(1,increment)
      }
      //判断 discountprice 数据类型
      val discount = jsonSku.get("discountprice")
      if (discount.isInstanceOf[Double]) {
        val discount = jsonSku.getDouble("discountprice")
      } else if (discount.isInstanceOf[Integer]) {
        val discount = jsonSku.getInt("discountprice")
      }

      val formatlasttime: DateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
      //向文档对象中插入新数据，存入Mongo
      doc.append("_id",jsonSku.getString("sku_id"))
      doc.append("item_id",jsonSku.getString("item_id").trim)
      doc.append("brand_productid",jsonSku.getString("brand_productid").trim)
      doc.append("color",colorlist)
      //add   sales,storeup,stock,commentcount,discountprice,lastseentime
      doc.append("sales",jsonSku.getInt("sales"))
      doc.append("storeup",jsonSku.getInt("storeup"))
      doc.append("stock",jsonSku.getInt("stock"))
      doc.append("commentcount",jsonSku.getInt("commentcount"))
      doc.append("discountprice",discount)
      doc.append("lastseentime",formatlasttime.format(new Date(jsonSku.getString("seentime")))) //格式2017/02/24 20:21:33
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
    //val otherinfo: JSONObject = jsonSku.getString("otherinfo").asInstanceOf[JSONObject]
    val otherinfo = new JSONObject(jsonSku.getString("otherinfo"))
    val iter: util.Iterator[String] = otherinfo.keys()
    //    val doc_other: Document = new Document() //创建文档对象
    var key: String = ""
    var value = ""
    while (iter.hasNext){
      key = iter.next()
      value = otherinfo.get(key).toString

      mongoCollection.updateOne(Filters.eq("_id", jsonSku.getString("sku_id").trim),new Document("$set", new Document("otherinfo", doc_other.append(key,value))))
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
    val moc = mongoCollection.find(Filters.eq("_id",jsonSku.getString("sku_id").trim)).first()
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
      //{"20170224" : [1,0],"20170226" : [2,1]}
      var finalkey = "" //20170226
      val iter:util.Iterator[String] = saleslistjson.keys()
      while (iter.hasNext){
        finalkey = iter.next()
      }
      //    不用数组，不用列表
      val salesStr: String = saleslistjson.get(finalkey).toString.replace("[","").replace("]","")
      //对于越界，加个判断
      var sales_old: String = null
      val index: Int = salesStr.indexOf(",")
      if(index != -1){
        sales_old = salesStr.substring(0,salesStr.indexOf(","))
      }else
      {
        return
      }
      val increment_new = Math.abs(jsonSku.getInt("sales") - (Integer.parseInt(sales_old)-Integer.parseInt(sales_old) / 30))
      list.add(0,jsonSku.getInt("sales"))
      list.add(1,increment_new)
      doc_update = doc_moc.append(seentime,list)
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("saleslist", doc_update)))

    }

    //*** 更新 lastseentime
    val formatlasttime: DateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
    if (jsonSku.getString("seentime") != "" && jsonSku.getString("seentime") != moc.get("lastseentime").toString){
      mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("lastseentime", formatlasttime.format(new Date(jsonSku.getString("seentime"))))))
    }

  }
  def main(args: Array[String]) {
//    val jsonstr = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/02/24 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":1,\"lastseentime\":\"2017/02/24 21:15:31\"}"
//    val json = new JSONObject(jsonstr)
//    toMongo(json)
//
//
//    val jsonstr1 = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/02/26 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":22,\"lastseentime\":\"2017/02/24 21:15:31\"}"
//    val json1 = new JSONObject(jsonstr1)
//    toMongo(json1)
//
//
//    val jsonstr2 = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/02/27 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":44,\"lastseentime\":\"2017/02/24 21:15:31\"}"
//    val json2 = new JSONObject(jsonstr2)
//    toMongo(json2)
//
//
//    val jsonstr3 = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/03/02 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":100,\"lastseentime\":\"2017/02/24 21:15:31\"}"
//    val json3 = new JSONObject(jsonstr3)
//    toMongo(json3)
//
//    val jsonstr4 = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/03/03 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":100,\"lastseentime\":\"2017/02/24 21:15:31\"}"
//    val json4 = new JSONObject(jsonstr4)
//    toMongo(json4)



    //    val a = "{id:[25555555,3454455454]}"
    //    val json = new JSONObject(a)
    //    var b: Array[String] = new Array[String](2)
    ////    b = json.get("id").toString.replace("[","").replace("]","").split(",")
    //    val c = json.get("id").toString.replace("[","").replace("]","")
    //    val d = c.substring(0,c.indexOf(","))
    //    println(d)
    //    val arrayfinal: util.List[Int] = new util.ArrayList[Int]()
    //    arrayfinal.add(0,Integer.parseInt(b(0)))
    //    arrayfinal.add(1,Integer.parseInt(b(1)))

    //    println(arrayfinal.get(0))
    //    println(arrayfinal.get(1))

  }
}
