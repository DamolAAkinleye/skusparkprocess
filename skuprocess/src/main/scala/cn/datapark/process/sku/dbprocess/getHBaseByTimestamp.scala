package cn.datapark.process.sku.dbprocess

import java.text.{SimpleDateFormat, DateFormat}
import java.util
import java.util.{Date, Calendar}

import cn.datapark.process.sku.config.{HBaseConfig, HbaseTableSchemas}
import cn.datapark.process.sku.hbase.HBaseClient
import cn.datapark.process.sku.util.{ConstantUtil, DistributedConfigUtil}
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.Document
import org.json.{JSONArray, JSONObject}

/**
  * 此程序是根据HBase中数据的时间戳范围进行取数据
  * 给定两个字符串时间，转换成13位的时间戳
  * 但是现在遇到的问题，时间间隔大，数据量大会造成运行不了
  * 待调试
  * Created by cluster on 2017/3/6.
  */
object getHBaseByTimestamp {
//  val dcu: DistributedConfigUtil = DistributedConfigUtil.getInstance
//  // *****获取HBase配置*****
//  val hbaseTableSchemas: HbaseTableSchemas = dcu.getDistributedConfigCollection.getHbaseTableSchemas
//  val hBaseConfig: HBaseConfig = dcu.getDistributedConfigCollection.getHBaseConfig
  val hBaseClient: HBaseClient = new HBaseClient
//  hBaseClient.initClient(hBaseConfig.getQuorum, hBaseConfig.getPort, hBaseConfig.getIsDistributed)
  hBaseClient.initClient("process3.pd.dp,process1.pd.dp,process2.pd.dp", "2181", "true")
  println("init HBase successfully")


  def main(args:Array[String]): Unit ={

//    // 本地模式运行
    val sparkConf = new SparkConf().setAppName("getHBaseByTimestampToMongo").setMaster("local[4]")
    //val sparkConf = new SparkConf().setAppName("HBaseToMongo")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
  val sc = new SparkContext(sparkConf)
  // 获取hbase表的命名空间和表名
//  hbaseTableSchemas.init("hbase_skudata_schema.xml")
//  val tableList = hbaseTableSchemas.getTableList

  import scala.collection.JavaConversions._

//  for (table <- tableList) {
//    if ((ConstantUtil.HBaseTable.HBaseTable_SkuData_Namespace == table.namespace) && (ConstantUtil.HBaseTable.HBaseTable_SkuData_TableName == table.tableName)) {
//      skuDataTable = table
//    }
//  }
//  namespace = skuDataTable.namespace
//  tableName = skuDataTable.tableName
  println("get HBase info successfully")

//  val minTime: String = "2017-03-06 00:00:05:000"
//  val maxTime: String = "2017-03-06 23:59:05:000"
//  val list: util.List[JSONObject]= hBaseClient.getbyScanTimeRange("dpa","skudata_0306",minTime,maxTime)
val list: util.List[JSONObject]= hBaseClient.getDatabyScan("dpa","skudata_0306")
  val listRDD = sc.parallelize(list, 20)// 将数据集切分成20份
  val rdd = listRDD.foreachPartition(part =>{
    part.foreach(line => {
//    HBaseByTimestampToMongo.toMongo(line)
      println(line)
    })

  })
  sc.stop()
}
}
//mintime: 1488782645000
//maxtime: 1488815945000
//scan 'dpa:skudata_0306', { TIMERANGE => [ 1488782645000, 1488815945000]}
