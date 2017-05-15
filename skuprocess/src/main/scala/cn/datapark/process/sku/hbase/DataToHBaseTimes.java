package cn.datapark.process.sku.hbase;

import cn.datapark.process.sku.config.HBaseConfig;
import cn.datapark.process.sku.config.HbaseTableSchemas;
import cn.datapark.process.sku.util.ConstantUtil;
import cn.datapark.process.sku.util.DistributedConfigUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 此类，是将传来的json数据，写入到 HBase中
 * 设计的rowkey为 散列值+时间（sdf7j-56y_20170302）
 * Created by cluster on 2017/2/7.
 */
public class DataToHBaseTimes implements Serializable {

    private static DistributedConfigUtil dcu = null;

    private static HBaseClient hbaseClient = null;

    private static HBaseConfig hBaseConfig = null;

    private static HbaseTableSchemas hbaseTableSchemas = null;


    private HbaseTableSchemas.Table skuDataTable = null;
    private String namespace = null;
    private String tableName = null;


    /**
     * 通过URL创建 HBase存储的RowKey的base64编码
     *
     * @param URL
     * @return
     */
    public static String buildHBaseRowkeyFromURL(String URL) {
        if ((URL == null) || URL.trim().equals("")) {
            return null;
        }
        //--------start--------add by yfh 2016-12-16 10:05:55
        if (URL.contains("&rn=")) {
            URL = URL.substring(0, URL.indexOf("&cat_id="));
        }
        //--------start--------add by yfh 2016-12-16 10:05:55
        try {
            final byte[] urlBytes = URL.getBytes("UTF-8");
            return Base64.getEncoder().encodeToString(urlBytes);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

//    /**
//     * 通过src_url的base64编码判断sku是否存在
//     *
//     * @param src_url
//     * @param nameSpace
//     * @param tableName
//     * @return
//     */
//    public static boolean isSKUExistbyURL(String src_url, String nameSpace, String tableName) {
//        dcu = DistributedConfigUtil.getInstance();
//
//        hBaseConfig = dcu.getDistributedConfigCollection().getHBaseConfig();
//
//        hbaseClient = new HBaseClient();
//
//        hbaseClient.initClient(hBaseConfig.getQuorum(), hBaseConfig.getPort(), hBaseConfig.getIsDistributed());
//
//        hbaseTableSchemas = dcu.getDistributedConfigCollection().getHbaseTableSchemas();
//        String HBaseRowKey = buildHBaseRowkeyFromURL(src_url);
//        Set<String> rowkeyset = hbaseClient.getRowkeybyScan(nameSpace, tableName);
//        boolean contains = rowkeyset.contains(HBaseRowKey);
//        return contains;
//    }


    public void ToHBase(JSONObject jsonobjScrapedSKU) throws IOException {
        dcu = DistributedConfigUtil.getInstance();

        hBaseConfig = dcu.getDistributedConfigCollection().getHBaseConfig();

        hbaseClient = new HBaseClient();

        hbaseClient.initClient(hBaseConfig.getQuorum(), hBaseConfig.getPort(), hBaseConfig.getIsDistributed());

        hbaseTableSchemas = dcu.getDistributedConfigCollection().getHbaseTableSchemas();

        //取出skuComment table
        List<HbaseTableSchemas.Table> tableList = hbaseTableSchemas.getTableList();


        for (HbaseTableSchemas.Table table : tableList) {
            if (ConstantUtil.HBaseTable.HBaseTable_SkuData_Namespace.equals(table.namespace) &&
                    ConstantUtil.HBaseTable.HBaseTable_SkuData_TableName.equals(table.tableName)) {
                skuDataTable = table;
            }
        }
        namespace = skuDataTable.namespace;
        tableName = skuDataTable.tableName;
        //此处不判断数据是否存在，按照时间戳为RowKey进行全量的存入
        //RowKey设计 散列_当前时间
        //long currentId = 1L;
        DateFormat format = new SimpleDateFormat("yyyyMMdd");
        String time = format.format(new Date());
//        byte [] rowkey = Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(currentId)).substring(0, 8).getBytes(),
//                Bytes.toBytes(currentId));
        String rowkey = UUID.randomUUID().toString().substring(0, 16) + "_" + time;
        //将数据存入HBase中
        Put put = new Put(Bytes.toBytes(rowkey));
        List<HbaseTableSchemas.ColumnFields> columnFieldsList = skuDataTable.columnFieldsList;
        for (HbaseTableSchemas.ColumnFields columnFields : columnFieldsList) {
            String columnName = columnFields.columnName;
            List<String> fields = columnFields.fields;
            for (String field : fields) {
                Object value = null;
                try {
                    if ("info".equals(columnName)) {
                        //在存入HBase的时候，判断：如果columnfamily为 "info" 且 对应的filed值为null
                        value = jsonobjScrapedSKU.isNull(field) ? null : jsonobjScrapedSKU.get(field);
                    } else {
                        value = jsonobjScrapedSKU.getJSONObject(columnName).get(field);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (value != null) {
                    if (value instanceof Integer)
                        put.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(field), Bytes.toBytes(String.valueOf(value)));
                    else if (value instanceof Double)
                        put.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(field), Bytes.toBytes(String.valueOf(value)));
                    else
                        put.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(field), Bytes.toBytes((value).toString()));
                }
                //如果value为null,则这个字段值不存
            }
        }
        hbaseClient.put(namespace, tableName, put);
    }

    public static void main(String[] args) {

//        String url = "https://detail.tmall.com/item.htm?id=19572525404&skuId=62499005099&sku=24477:20532&user_id=893521147&cat_id=50542039&is_b=1&rn=9b4897d3ca2aa377220e1ac9db484045";
////        String RowKey = buildHBaseRowkeyFromURL(url);
////        System.out.println(RowKey);
////        System.out.println(isSKUExistbyURL(url,"dpa","skudatatest_0209","info","discountlist"));
////
////        dcu = DistributedConfigUtil.getInstance();
////        hBaseConfig = dcu.getDistributedConfigCollection().getHBaseConfig();
////        hbaseClient = new HBaseClient();
////        hbaseClient.initClient(hBaseConfig.getQuorum(),hBaseConfig.getPort(),hBaseConfig.getIsDistributed());
////        hbaseTableSchemas = dcu.getDistributedConfigCollection().getHbaseTableSchemas();
////        hbaseClient = new HBaseClient();
//
//        System.out.println(isSKUExistbyURL(url,"dpa","skudatatest_0209"));
        long currentId = 1L;
        DateFormat format = new SimpleDateFormat("yyyyMMdd");
        String time = format.format(new Date());
        byte[] rowkey = Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(currentId)).substring(0, 8).getBytes(), Bytes.toBytes(time));
        String a = UUID.randomUUID().toString().substring(0, 16) + "_" + time;
        System.out.println(new String(rowkey));
        System.out.println(a);

    }


}
