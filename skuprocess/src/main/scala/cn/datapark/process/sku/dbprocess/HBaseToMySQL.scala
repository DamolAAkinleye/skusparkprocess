package cn.datapark.process.sku.dbprocess

import java.sql.Connection
import java.sql.DriverManager

import java.sql.PreparedStatement
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by seagle on 6/28/16.
  * spark sql 1.6.1
  * 查询 HBase中数据,注册成表写入 MySQL
  *
  */
object HBaseToMySQL {
  //过滤日志
  Logger.getLogger("org").setLevel(Level.ERROR)

  //存数据到MySQL
  case class Sku(color_alias: String, brand: String)
  var conn: Connection = null
  var ps: PreparedStatement = null
  val sql = "insert into sku_color_brand(color_alias,brand) values (?,?)"


  def main(args:Array[String]): Unit ={
    // 本地模式运行,便于测试
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseTest")
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE,"dpa:skudatatest_0214")
    hBaseConf.set("hbase.zookeeper.quorum","192.168.31.61,192.168.31.62,192.168.31.63")
    //设置zookeeper连接端口，默认2181
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val shop = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("src_url"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("src_name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tags"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("desc"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("shelvetime"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("markettime"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("seentime"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("pd_src_img_url"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("marketprice"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("discountprice"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("color"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("color_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("material"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("material_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("fashionelements"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("print"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("print_alias"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("length"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tech")))

      ))//.
      .toDF("src_url","src_name","name","tags","desc","shelvetime","markettime","seentime","pd_src_img_url","marketprice","discountprice","brand","brand_alias","color","color_alias","material","material_alias","fashionelements","print","print_alias","length","tech")
    shop.registerTempTable("skuData")
    // 测试
    val df2 = sqlContext.sql("SELECT color_alias, brand FROM skuData").rdd

    val resultRDD = df2.map(row =>{
      (row.getString(0),row.getString(1))
    })
    resultRDD.foreachPartition(iterator => {
      try {
        conn = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/cpeixinTest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
        iterator.foreach(line=>{
          ps = conn.prepareStatement(sql)
          if (line._1 != null && line._2 != null){
            ps.setString(1,line._1)
            ps.setString(2, line._2)
            ps.executeUpdate()
          }
        })
      } catch {
        case e: Exception =>  println(e.printStackTrace())
      }finally {
        if (ps != null)
          ps.close()
        if (conn != null)
          conn.close()
      }
    })
    //存数据到MySQL
    sc.stop()

  }
}