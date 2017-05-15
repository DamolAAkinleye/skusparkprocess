import java.text.{SimpleDateFormat, DateFormat}
import java.util
import java.util.{Collections, Date}
import cn.datapark.process.sku.config.{HBaseConfig, HbaseTableSchemas}
import cn.datapark.process.sku.hbase.HBaseClient
import cn.datapark.process.sku.util.{ConstantUtil, DistributedConfigUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Count}
import org.json.JSONObject


/**
  * Created by cluster on 2017/2/20.
  */
object Test {

  //获取变量的数据类型
  def getType(o: AnyRef): String = {
    o.getClass.toString
  }
  //  val dcu: DistributedConfigUtil = DistributedConfigUtil.getInstance
  //  // *****获取HBase配置*****
  //  val hbaseTableSchemas: HbaseTableSchemas = dcu.getDistributedConfigCollection.getHbaseTableSchemas
  //  val hBaseConfig: HBaseConfig = dcu.getDistributedConfigCollection.getHBaseConfig
  //  val hBaseClient: HBaseClient = new HBaseClient
  //  hBaseClient.initClient(hBaseConfig.getQuorum, hBaseConfig.getPort, hBaseConfig.getIsDistributed)
  //  var skuDataTable: HbaseTableSchemas#Table = null
  //  var namespace: String = null
  //  var tableName: String = null

  val formatlasttime: DateFormat = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
  val format: DateFormat = new SimpleDateFormat("yyyyMMdd")//将时间转换成这种格式
  def main(args: Array[String]) {
    //spark 2.0的配置
        val warehouseLocation = "/spark-warehouse"
        val spark = SparkSession
          .builder().master("local[2]")
          .appName("test DataSet")
          .config("spark.sql.warehouse.dir", warehouseLocation)
          .config("spark.default.parallelism", "100")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", 6)
        spark.conf.set("spark.executor.memory", "2g")

    //    //创建DataSet
    //    val tableList = hbaseTableSchemas.getTableList
    //    import scala.collection.JavaConversions._
    //    for (table <- tableList) {
    //      if ((ConstantUtil.HBaseTable.HBaseTable_SkuData_Namespace == table.namespace) && (ConstantUtil.HBaseTable.HBaseTable_SkuData_TableName == table.tableName)) {
    //        skuDataTable = table
    //      }
    //    }
    //    namespace = skuDataTable.namespace
    //    tableName = skuDataTable.tableName
    //
    //    val byte = hBaseClient.getCellValueByRowkey("ed7283e1-9fa5-4aa7-b395-541bb259e793","dpa","skudata_0306","info","otherinfo")
    //    println(new String(byte))

    val jsonstr = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/02/24 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":1,\"lastseentime\":\"2017/02/24 21:15:31\"}"
    val json = new JSONObject(jsonstr)
//        toMongo(json)


    val jsonstr1 = "{\"brand_productid\":\" V5937\",\"color_alias\":\"[\\\"绿\\\"]\",\"commentcount\":1445,\"otherinfo\":\"{\\\"厚薄\\\":\\\" 常规\\\",\\\"基础风格\\\":\\\" 时尚都市\\\",\\\"适用场景\\\":\\\" 日常\\\",\\\"面料分类\\\":\\\" 其他\\\",\\\"袖型\\\":\\\" 常规\\\",\\\"销售渠道类型\\\":\\\" 纯电商(只在线上销售)\\\",\\\"适用对象\\\":\\\" 中年\\\",\\\"袖长\\\":\\\" 短袖\\\",\\\"细分风格\\\":\\\" 商务休闲\\\",\\\"款式细节\\\":\\\" 其他\\\",\\\"印花主题\\\":\\\" 几何图案\\\",\\\"服饰工艺\\\":\\\" 水洗\\\"}\",\"item_id\":\"44108251507\",\"sku_id\":\"3188188027170\",\"discountprice\":99,\"shelvetime\":\"2017/02/26 21:15:31\",\"stock\":330,\"storeup\":988,\"sales\":22,\"lastseentime\":\"2017/02/24 21:20:31\"}"
    val json1 = new JSONObject(jsonstr1)
    //    toMongo(json1)


    val jsontime_long: Date = new Date(formatlasttime.format(new Date(json.getString("lastseentime"))))
    val jsontime_short = format.format(new Date(json.getString("lastseentime")))

//    println(jsontime_long)
//    println(jsontime_short)

    val json1time_long: Date = new Date(formatlasttime.format(new Date(json1.getString("lastseentime"))))
    val json1time_short = format.format(new Date(json1.getString("lastseentime")))

//    println(getType(json1time_long))
//    println(json1time_short)

//    if (jsontime_short == json1time_short){
//      println("相等")
//    }else println("不相等")

//      if (jsontime_long.getTime > json1time_long.getTime){
//           println("21:15:31大于21:20:31")
//      }else println("21:20:31大于21:15:31")
    if ((json1time_short != jsontime_short) || (jsontime_short == json1time_short && json1time_long.getTime > jsontime_long.getTime)){

    }


  }
}