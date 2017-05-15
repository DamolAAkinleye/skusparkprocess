package cn.datapark.process.sku.processor

import java.util
import cn.datapark.process.sku.dbprocess.SparkToMongo
import cn.datapark.process.sku.dbprocess.SparkToMongo._
import cn.datapark.process.sku.hbase.{DataToHBaseTimes, DataToHBase, HBaseClient}

import cn.datapark.process.sku.preprocess._

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.json.JSONObject

//import net.sf.json.{JSONArray, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkException, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cluster on 2017/3/1.
  */
object skuprocess2mongo1 extends Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)




  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("skuprocess2mongo1")
    //    val conf = new SparkConf().setAppName("skuprocess2mongo1")
    //加入解决序列化问题
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    val ssc = new StreamingContext(conf, Seconds(15))
    val topics = Set("skurawdataonline_new02")
    val brokers = "process1.pd.dp:6667,process6.pd.dp:6667,process7.pd.dp:6667"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "group.id" -> "test-consumer-group","serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "smallest")

    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val processRDD = kafkaDStream.flatMap(line =>{
      val dataAlias = addFieldAlias.FieldAlias(new JSONObject(line._2))
      val dataField = FormatProcess.Formatfield(dataAlias)
      val dataList = FormatProcess.formatNewSKU(dataField)
      Some(dataList)
    })

    processRDD.foreachRDD(rdd =>{
      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(line => line.foreach(json => {
        SparkToMongo.toMongo(json)
      }))
//      km.updateZKOffsets(rdd.toString())
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
