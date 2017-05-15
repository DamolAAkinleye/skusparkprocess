package cn.datapark.process.sku.processor

import java.util
import cn.datapark.process.sku.config.HBaseConfig
import cn.datapark.process.sku.config._
import cn.datapark.process.sku.hbase.{DataToHBaseTimes, DataToHBase, HBaseClient}
import cn.datapark.process.sku.preprocess.{addFieldAlias, BrandMatch, FieldAliasProcessor, FormatProcess}
import cn.datapark.process.sku.util.DistributedConfigUtil
import kafka.serializer.StringDecoder
import org.json.JSONObject

//import net.sf.json.{JSONArray, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cluster on 2017/2/5.
  */
object skuprocess extends Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val dcu: DistributedConfigUtil = DistributedConfigUtil.getInstance
  //获取HBase配置
  val hbaseTableSchemas: HbaseTableSchemas = dcu.getDistributedConfigCollection.getHbaseTableSchemas

  //init hbase
  val hBaseConfig: HBaseConfig = dcu.getDistributedConfigCollection.getHBaseConfig
  val hBaseClient: HBaseClient = new HBaseClient
  hBaseClient.initClient(hBaseConfig.getQuorum, hBaseConfig.getPort, hBaseConfig.getIsDistributed)


  def main(args: Array[String]) {

    //创建HBase 表
//    val tableList: util.List[HbaseTableSchemas#Table] = hbaseTableSchemas.getTableList
    val tableList = hbaseTableSchemas.getTableList
    import scala.collection.JavaConversions._
    for (table <- tableList) {
      val namespace: String = table.namespace
      val tableName: String = table.tableName
      val columnFaimlyList: util.List[String] = table.getColumnFaimlyList
      hBaseClient.createTable(namespace, tableName, columnFaimlyList)
    }

    //创建Hbase对象
    val dataToHBase: DataToHBaseTimes = new DataToHBaseTimes

    val conf = new SparkConf().setMaster("local[2]").setAppName("skuprocess")

    //加入解决序列化问题
    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = Set("skurawdataonline_new02")
    val brokers = "process1.pd.dp:6667,process6.pd.dp:6667,process7.pd.dp:6667"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "smallest")
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //  获取kafka中的原数据并进行处理
    val dataDStream = kafkaDStream.flatMap(line => {
      val dataAlias = addFieldAlias.FieldAlias(new JSONObject(line._2)) //第一步：对原始数据中的color,print,brand,material添加别名
      val dataField = FormatProcess.Formatfield(dataAlias) //第二步：重新获取并替换storeup，sales，commentcount，stock的值
      val dataList = FormatProcess.formatNewSKU(dataField) //第三步：格式化sku数据  设置discountlist，storeuplist，saleslist，commentcountlist，stocklist
      dataToHBase.ToHBase(dataList) //第四步：数据写入HBase
      Some(dataList)
    })
    //  打印获取的原数据
    val data1DStream = dataDStream.foreachRDD(rdd => {
      rdd.foreach(msg => {
        println(msg)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
