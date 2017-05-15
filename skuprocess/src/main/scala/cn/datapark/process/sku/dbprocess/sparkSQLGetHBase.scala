package cn.datapark.process.sku.dbprocess

import cn.datapark.process.sku.config.{HBaseConfig, HbaseTableSchemas}
import cn.datapark.process.sku.hbase.HBaseClient
import cn.datapark.process.sku.util.{ConstantUtil, DistributedConfigUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.json.JSONObject

/**
  * spark sql 2.0
  * 在HBase中查询出指定的数据 ，注册成表
  * 利用sql语句将我们想查询的数据查出来,并且将每条数据转化成 json 格式
  * 存入 Mongo
  * Created by cluster on 2017/3/6.
  */
object sparkSQLGetHBase extends Serializable{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val dcu: DistributedConfigUtil = DistributedConfigUtil.getInstance
  // *****获取HBase配置*****
  val hbaseTableSchemas: HbaseTableSchemas = dcu.getDistributedConfigCollection.getHbaseTableSchemas
  val hBaseConfig: HBaseConfig = dcu.getDistributedConfigCollection.getHBaseConfig
  var skuDataTable: HbaseTableSchemas#Table = null
  var namespace: String = null
  var tableName: String = null

  def main(args:Array[String]): Unit ={
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
//    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
//      .builder().master("local[4]")  //本地运行
      .builder()
      .appName("sparkSQL  HBase")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.default.parallelism", "100")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取hbaseTableSchemas中的信息
    val tableList = hbaseTableSchemas.getTableList
    import scala.collection.JavaConversions._
    for (table <- tableList) {
      if ((ConstantUtil.HBaseTable.HBaseTable_SkuData_Namespace == table.namespace) && (ConstantUtil.HBaseTable.HBaseTable_SkuData_TableName == table.tableName)) {
        skuDataTable = table
      }
    }
    namespace = skuDataTable.namespace
    tableName = skuDataTable.tableName
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE,namespace+":"+tableName)
    hBaseConf.set("hbase.zookeeper.quorum",hBaseConfig.getQuorum)
    //设置zookeeper连接端口，默认2181
    hBaseConf.set("hbase.zookeeper.property.clientPort", hBaseConfig.getPort)
    hBaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hBaseConf.set("hbase.client.retries.number", "3")

    import spark.implicits._
    // 从数据源获取数据
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"),Bytes.toBytes("sku_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"),Bytes.toBytes("item_id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("id"),Bytes.toBytes("brand_productid"))),
      //Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("color_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("shelvetime"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("stock"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("sales"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("storeup"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("commentcount"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("otherinfo"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("discountprice"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("seentime"))),
      //03.17 add 新字段
      Bytes.toString(r._2.getValue(Bytes.toBytes("category"),Bytes.toBytes("indextype"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("category"),Bytes.toBytes("description"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("category"),Bytes.toBytes("style"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("material_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("print_alias"))),
//      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("style_alias"))),
//      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("model_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("collarmodel"))),
      //03.22 add新字段
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("src_name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_alias")))
//      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("sleevemodel_alias"))),
      )).toDF("sku_id","item_id","brand_productid","color_alias","shelpvetime","stock","sales","storeup","commentcount","otherinfo","discountprice","seentime","category_indextype","category_description","category_style","material_alias","print_alias","collarmodel","src_name","brand_alias")
      .registerTempTable("sku")
    //
    val df2 = spark.sql("SELECT * FROM sku")

    val jsonRDD = df2.rdd.map(row =>{
      val json = new JSONObject()
      json.put("sku_id",row.getString(0))
      json.put("item_id",row.getString(1))
      json.put("brand_productid",row.getString(2).replace(" ",""))
      //      json.put("brand_alias",row.getString(0))
      json.put("color_alias",row.getString(3))
      json.put("shelvetime",row.get(4).toString)
      json.put("stock",Integer.parseInt(row.get(5).toString))
      json.put("sales",Integer.parseInt(row.get(6).toString))
      json.put("storeup",Integer.parseInt(row.get(7).toString))
      json.put("commentcount",Integer.parseInt(row.get(8).toString))
      json.put("otherinfo",row.getString(9))
      //转换 discountprice 的数据类型        /*考虑用模式匹配去改写*/
      if (row.getString(10).contains(".")){
        val discountprice = row.getString(10).toDouble
        json.put("discountprice",discountprice)
      }else {
        val discountprice = row.getString(10).toInt
        json.put("discountprice",discountprice)
      }
      // hbase中 没有 lasttime这个值  ， 用seentime代替
      if (row.getString(11) != "" && row.getString(11) != null){
        json.put("lastseentime",row.getString(11))
      }else {
        json.put("lastseentime","")
      }
      //"category_indextype","category_description","category_style","material_alias","print_alias","collarmodel"
      json.put("category_indextype",row.getString(12))
      json.put("category_description",row.getString(13))
      json.put("category_style",row.getString(14))
      //判断为空
      if (row.getString(15) != "" && row.getString(15) != null){
        json.put("material_alias",row.getString(15))
      } else {
        json.put("material_alias","暂无")
      }
      //判断为空
      if (row.getString(16) != "" && row.getString(16) != null){
        json.put("print_alias",row.getString(16))
      } else {
        json.put("print_alias","暂无")
      }
      //判断为空
      if (row.getString(17) != "" && row.getString(17) != null){
        json.put("collarmodel",row.getString(17))
      } else {
        json.put("collarmodel","暂无")
      }
      // add 03.22
      json.put("src_name",row.getString(18))//平台
      json.put("brand_alias",row.getString(19))//品牌
      json
    })
    jsonRDD.repartition(200).foreachPartition(part =>{
           part.foreach(line => SparkSQLHBToMongo.toMongo(line))
//      part.foreach(line => println(line))
    })
    spark.stop()
  }

}
