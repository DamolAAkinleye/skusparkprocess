package cn.datapark.process.sku.dbprocess
import java.text.{SimpleDateFormat, DateFormat}
import java.util
import java.util.{Calendar, Date}
import cn.datapark.process.sku.config.{HBaseConfig, HbaseTableSchemas}
import cn.datapark.process.sku.hbase.HBaseClient
import cn.datapark.process.sku.util.{ConstantUtil, DistributedConfigUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.{JSONArray, JSONObject}


import com.mongodb.MongoClient
import com.mongodb.client.model.Filters
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.bson.Document

/**
  * 使用spark
  * 从HBase中获取前一天的数据
  * 存入Mongo
  *
  * Created by cluster on 2017/2/13.
  */
object HBaseToMongo extends Serializable{

  val dcu: DistributedConfigUtil = DistributedConfigUtil.getInstance
  // *****获取HBase配置*****
  val hbaseTableSchemas: HbaseTableSchemas = dcu.getDistributedConfigCollection.getHbaseTableSchemas
  val hBaseConfig: HBaseConfig = dcu.getDistributedConfigCollection.getHBaseConfig
  val hBaseClient: HBaseClient = new HBaseClient
  hBaseClient.initClient(hBaseConfig.getQuorum, hBaseConfig.getPort, hBaseConfig.getIsDistributed)
  var skuDataTable: HbaseTableSchemas#Table = null
  var namespace: String = null
  var tableName: String = null

  // *****获取Mongo的连接*****
  //2. //通过连接认证获取MongoDB连接
  val mongoClient: MongoClient  = new MongoClient("192.168.39.96", 27017)
  //连接到数据库
  val mongoDatabase: MongoDatabase  = mongoClient.getDatabase("SKU_test")
  println("Connect to database successfully")
  //获取集合 参数为“集合名称”
  val mongoCollection: MongoCollection[Document]  = mongoDatabase.getCollection("sku_change_test")
  println("Connect to Collection successfully")

  //存放src_url
  val set_srcurl = new util.HashSet[String]()

  def main(args:Array[String]): Unit ={

    // 本地模式运行
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseToMongo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
    val sc = new SparkContext(sparkConf)
    // 获取hbase表的命名空间和表名
    val tableList = hbaseTableSchemas.getTableList
    import scala.collection.JavaConversions._
    for (table <- tableList) {
      if ((ConstantUtil.HBaseTable.HBaseTable_SkuData_Namespace == table.namespace) && (ConstantUtil.HBaseTable.HBaseTable_SkuData_TableName == table.tableName)) {
        skuDataTable = table
      }
    }
    namespace = skuDataTable.namespace
    tableName = skuDataTable.tableName

    //获取当前时间的前一天，作为获取数据的正则规则
    val timeReg = getbeforeDay()
    val list: util.List[JSONObject]= hBaseClient.getRowkeybyScanCompareOp(".*"+timeReg,namespace,tableName)
    val listRDD = sc.parallelize(list)
    val columnFieldsList = skuDataTable.columnFieldsList
        val rdd = listRDD.foreach(elem => {
          if (isSKUExist(elem.getString("src_url"))){// ***添加判断sku是否存在
            // ***如果存在，则替换对应字段值
            updateExistSKU(elem)
          }else {// ***如果不存在，则直接存入MongoDB中
            val iter: util.Iterator[String] = elem.keys()
            //创建文档对象
            val doc: Document = new Document()
            while (iter.hasNext){
              val key = iter.next()
              val value = elem.get(key)
              doc.append(key,value)
            }
            mongoCollection.insertOne(doc)
            println("===insert successfully===")
          }
          println(elem)
        })
    sc.stop()
  }
  /**
    * 获取当前时间的前一天
    *
    * @return
    */
  def getbeforeDay(): String ={
      val calendar: Calendar = Calendar.getInstance
      //add()中，第一个参数是当前时间，第二个参数数字代表的是 天数的变化量， -1提前一天，0当前时间，1后一天
      calendar.add(Calendar.DATE, 0)
      val date: Date = calendar.getTime
      val df: DateFormat = new SimpleDateFormat("yyyyMMdd")
      df.format(date)
    }
  /**
    * 判断要存入的json数据是否存在
    *
    * @param src_url json数据中的url
    * @return 返回布尔类型数据
    */
  def isSKUExist(src_url: String): Boolean={
    //获取所有数据
    val allData = mongoCollection.find()
    //遍历获取每条数据的src_url
    val it = allData.iterator()
    while (it.hasNext){
      val src = it.next().getString("src_url")
      set_srcurl.add(src)
    }
    //判断src_url是否存在
    val boolean = set_srcurl.contains(src_url)
    boolean
  }
  /**
    * 更新Mongo中已经存在的数据字段 discountlist,sales,stock,storeup,comment
    * @param json hbase中获取的json数据
    */
  def updateExistSKU(json: JSONObject): Unit ={
    // ***处理discountlist字段
    val moc = mongoCollection.find(Filters.eq("src_url",json.getString("src_url"))).first()
    val discountlist = moc.getString("discountlist")//mongo中获取的discountList字段值
    val jsonarryDiscountList = new JSONArray(discountlist)//mongo中获取的discountList字段值转化成JSONArray，以便接下来的操作
    val jsondiscountList = jsonarryDiscountList.get(jsonarryDiscountList.length() - 1).asInstanceOf[JSONObject]//获取JSONArray中最后一段{price.time}数据
    val discountList_price_m = jsondiscountList.get("price")//mongo中的discountList中的price值
    val discountprice = json.get("discountprice") //从HBase 中取出来的Json数据的discountprice值
    //如果discountList中的price和json中的discountprice不相等，则更新
    if (discountList_price_m.toString != discountprice.toString){
      val discountList_add  = new JSONObject()
      //判断 discountprice 的类型
      if (discountprice.isInstanceOf[Integer]){
        val discountpriceformat = json.getInt("discountprice").asInstanceOf[Double]
        discountList_add.put("price",discountpriceformat)
        discountList_add.put("time",json.getString("lastseentime"))
        val jsonarryDiscountList_new = jsonarryDiscountList.put(discountList_add)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountlist", jsonarryDiscountList_new.toString)))
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountprice", discountpriceformat)))
        println("===update successfully===")
      }else{
        val discountpriceformat = json.get("discountprice")
        discountList_add.put("price",discountpriceformat)
        discountList_add.put("time",json.getString("lastseentime"))
        val jsonarryDiscountList_new = jsonarryDiscountList.put(discountList_add)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountlist", jsonarryDiscountList_new.toString)))
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("discountprice", discountpriceformat)))
        println("===update successfully===")
      }
    }
    // ***处理sales字段
    if (json.getString("sales") != "" && json.getString("sales") != null){
      val sales_Json = json.getString("sales") //json中的sales值
      val saleslist_mongo = moc.getString("saleslist") //mongo中saleslist的值
      val jsonarrysaleslist = new JSONArray(saleslist_mongo) //将mongo中saleslist的值转换成JSONArray格式，以便接下来的操作
      val jsonsaleslist = jsonarrysaleslist.get(jsonarrysaleslist.length() - 1).asInstanceOf[JSONObject] //获取JSONArray中最后一段{number,increment,time}数据
      val number_saleslist = jsonsaleslist.getInt("number")
      //判断json sales和 mongo saleslist number是否相等
      if(sales_Json.toString != number_saleslist.toString){
        val saleslist_new  = new JSONObject()
        saleslist_new.put("number",Integer.parseInt(sales_Json))
        saleslist_new.put("increment", Math.abs(Integer.parseInt(sales_Json) - (number_saleslist-number_saleslist / 30)))
        saleslist_new.put("time",json.getString("lastseentime"))
        val jsonarrysaleslist_new = jsonarrysaleslist.put(saleslist_new)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("sales", Integer.parseInt(sales_Json))))
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("saleslist", jsonarrysaleslist_new.toString)))
        println("===update successfully===")
      }
    }
    // ***处理stock字段
    if (json.getString("stock") != "" && json.getString("stock") != null){
      val stock_Json = json.getString("stock")
      val stocklist_mongo = moc.getString("stocklist")
      val jsonarrystocklist = new JSONArray(stocklist_mongo)
      val jsonstocklist = jsonarrystocklist.get(jsonarrystocklist.length() - 1).asInstanceOf[JSONObject] //获取JSONArray中最后一段{number,increment,time}数据
      val number_stocklist = jsonstocklist.getInt("number")
      //判断json sales和 mongo saleslist number是否相等
      if(stock_Json != number_stocklist.toString){
        val stocklist_new  = new JSONObject()
        stocklist_new.put("number",Integer.parseInt(stock_Json))
        stocklist_new.put("time",json.getString("lastseentime"))
        val jsonarrystocklist_new = jsonarrystocklist.put(stocklist_new)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("stock", Integer.parseInt(stock_Json))))
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("stocklist", jsonarrystocklist_new.toString)))
        println("===update successfully===")
      }
    }
    // ***处理storeup字段
    if (json.getString("storeup") != "" && json.getString("storeup") != null){
      val storeup_Json = json.getString("storeup")
      val storeuplist_mongo = moc.getString("storeuplist")
      val jsonarrystoreuplist = new JSONArray(storeuplist_mongo)
      val jsonstoreuplist = jsonarrystoreuplist.get(jsonarrystoreuplist.length() - 1).asInstanceOf[JSONObject] //获取JSONArray中最后一段{number,increment,time}数据
      val number_storeuplist = jsonstoreuplist.getInt("number")
      //判断json sales和 mongo saleslist number是否相等
      if(storeup_Json != number_storeuplist.toString){
        val storeuplist_new  = new JSONObject()
        storeuplist_new.put("number",Integer.parseInt(storeup_Json))
        storeuplist_new.put("time",json.getString("lastseentime"))
        val jsonarrystoreuplist_new = jsonarrystoreuplist.put(storeuplist_new)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("storeup", Integer.parseInt(storeup_Json))))
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("storeuplist", jsonarrystoreuplist_new.toString)))
        println("===update successfully===")
      }
    }
    // ***处理commentcount字段
    if (json.getString("commentcount") != "" || json.getString("commentcount") != null){
      val commentcount_Json = json.getString("commentcount")
      val commentcountlist_mongo = moc.getString("commentcountlist")
      val jsonarrycommentcountlist = new JSONArray(commentcountlist_mongo)
      val jsoncommentcountlist = jsonarrycommentcountlist.get(jsonarrycommentcountlist.length() - 1).asInstanceOf[JSONObject] //获取JSONArray中最后一段{number,increment,time}数据
      val number_commentcountlist = jsoncommentcountlist.getInt("number")
      //判断json sales和 mongo saleslist number是否相等
      if(commentcount_Json != number_commentcountlist.toString){
        val commentcountlist_new  = new JSONObject()
        commentcountlist_new.put("number",Integer.parseInt(commentcount_Json))
        commentcountlist_new.put("time",json.getString("lastseentime"))
        val jsonarrycommentcountlist_new = jsonarrycommentcountlist.put(commentcountlist_new)
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("commentcount", Integer.parseInt(commentcount_Json))))
        mongoCollection.updateOne(Filters.eq("_id", moc.get("_id")),new Document("$set", new Document("commentcountlist", jsonarrycommentcountlist_new.toString)))
        println("===update successfully===")
      }
    }
  }
}





