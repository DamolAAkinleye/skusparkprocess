package cn.datapark.process.sku.hbase;

import cn.datapark.process.sku.config.HBaseConfig;
import cn.datapark.process.sku.config.HbaseTableSchemas;
import cn.datapark.process.sku.util.ConstantUtil;
import cn.datapark.process.sku.util.DistributedConfigUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;
import java.util.Set;

/**
 *  此类，是将传来的json数据，写入到 HBase中
 * Created by cluster on 2017/2/7.
 */
public class DataToHBase implements Serializable {

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

    /**
     * 通过src_url的base64编码判断sku是否存在
     *
     * @param src_url
     * @param nameSpace
     * @param tableName
     * @return
     */
    public static boolean isSKUExistbyURL(String src_url, String nameSpace, String tableName) {
        dcu = DistributedConfigUtil.getInstance();

        hBaseConfig = dcu.getDistributedConfigCollection().getHBaseConfig();

        hbaseClient = new HBaseClient();

        hbaseClient.initClient(hBaseConfig.getQuorum(), hBaseConfig.getPort(), hBaseConfig.getIsDistributed());

        hbaseTableSchemas = dcu.getDistributedConfigCollection().getHbaseTableSchemas();
        String HBaseRowKey = buildHBaseRowkeyFromURL(src_url);
        Set<String> rowkeyset = hbaseClient.getRowkeybyScan(nameSpace, tableName);
        boolean contains = rowkeyset.contains(HBaseRowKey);
        return contains;
    }


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

        //判断 HBase中是否存在这个sku
        if (isSKUExistbyURL(jsonobjScrapedSKU.getString("src_url"), namespace, tableName)) {
            //如果存在  添加存在的处理
            //获取HBase实例
            dcu = DistributedConfigUtil.getInstance();
            hBaseConfig = dcu.getDistributedConfigCollection().getHBaseConfig();
            hbaseClient = new HBaseClient();
            hbaseClient.initClient(hBaseConfig.getQuorum(), hBaseConfig.getPort(), hBaseConfig.getIsDistributed());
            hbaseTableSchemas = dcu.getDistributedConfigCollection().getHbaseTableSchemas();
            hbaseClient = new HBaseClient();

            String HBaseRowKey = buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"));//获取对应的Rowkey
            byte[] discountlist = hbaseClient.getCellValueByRowkey(HBaseRowKey, namespace, tableName, "info", "discountlist");//获取discountlist字段值
            byte[] saleslist = hbaseClient.getCellValueByRowkey(HBaseRowKey, namespace, tableName, "info", "saleslist");//获取saleslist字段值
            byte[] storeuplist = hbaseClient.getCellValueByRowkey(HBaseRowKey, namespace, tableName, "info", "storeuplist");//获取storeuplist字段值
            byte[] stocklist = hbaseClient.getCellValueByRowkey(HBaseRowKey, namespace, tableName, "info", "stocklist");//获取stocklist字段值
            byte[] commentcountlist = hbaseClient.getCellValueByRowkey(HBaseRowKey, namespace, tableName, "info", "commentcountlist");//获取commentcountlist字段值

            //处理discountlist字段
            JSONArray discountjsonArray = new JSONArray(new String(discountlist));//将discountlist字段值转换成JSONArray类型 eg:[{"price":200,"time":"2017/02/03 10:28:07"}]
            JSONObject discountjson = (JSONObject) discountjsonArray.get(discountjsonArray.length() - 1);//获取discountlist中最后的一个数组值 eg:[{},{}]
            String pricediscountlist = discountjson.get("price").toString();//获取HBase中discountlist中的price值
            String discountprice = jsonobjScrapedSKU.get("discountprice").toString();//json数据中的discountprice字段值
            //比较HBase中的price字段和Json数据中的discountprice字段的值
            if (pricediscountlist != discountprice) {
                JSONObject discountlistjson = new JSONObject();//新建在discountlist字段追加一条信息{"price":XX,"time":"YYYY-MM-SS"}
                discountlistjson.put("price", discountprice);
                discountlistjson.put("time", jsonobjScrapedSKU.getString("lastseentime"));
                discountjsonArray.put(discountlistjson);//追加在discountlist中
                Put put = new Put(Bytes.toBytes(buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"))));
                //如果不相等，就更新HBase中discountprice字段
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("discountprice"), Bytes.toBytes(String.valueOf(discountprice)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("discountlist"), Bytes.toBytes(String.valueOf(discountjsonArray)));
                hbaseClient.put(namespace, tableName, put);
            }
            //处理saleslist字段
            JSONArray salesjsonArray = new JSONArray(new String(saleslist));
            JSONObject salesjson = (JSONObject) salesjsonArray.get(salesjsonArray.length() - 1);
            int saleslistNumber = (Integer) salesjson.get("number");
            int sales = (Integer) jsonobjScrapedSKU.get("sales");
            //比较HBase中的saleslist--number字段和Json数据中的sales字段的值
            if (saleslistNumber != sales) {
                JSONObject salesjsonToList = new JSONObject();
                salesjsonToList.put("number", sales);
                salesjsonToList.put("increment", Math.abs(sales - (saleslistNumber - saleslistNumber / 30)));
                salesjsonToList.put("time", jsonobjScrapedSKU.getString("lastseentime"));
                salesjsonArray.put(salesjsonToList);
                Put put = new Put(Bytes.toBytes(buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"))));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sales"), Bytes.toBytes(String.valueOf(sales)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("saleslist"), Bytes.toBytes(String.valueOf(salesjsonArray)));
                hbaseClient.put(namespace, tableName, put);
            }
            //处理storeuplist字段
            JSONArray storeupjsonArray = new JSONArray(new String(storeuplist));
            JSONObject storeupjson = (JSONObject) storeupjsonArray.get(storeupjsonArray.length() - 1);
            int storeupListNumber = (Integer) storeupjson.get("number");
            int storeup = (Integer) jsonobjScrapedSKU.get("storeup");
            //比较HBase中的storeuplist--number字段和Json数据中的storeup字段的值
            if (storeupListNumber != storeup) {
                JSONObject storeupjsonToList = new JSONObject();
                storeupjsonToList.put("number", storeup);
                storeupjsonToList.put("time", jsonobjScrapedSKU.getString("lastseentime"));
                storeupjsonArray.put(storeupjsonToList);
                Put put = new Put(Bytes.toBytes(buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"))));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("storeup"), Bytes.toBytes(String.valueOf(storeup)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("storeuplist"), Bytes.toBytes(String.valueOf(storeupjsonArray)));
                hbaseClient.put(namespace, tableName, put);
            }
            //处理stocklist字段
            JSONArray stockjsonArray = new JSONArray(new String(stocklist));
            JSONObject stockjson = (JSONObject) stockjsonArray.get(stockjsonArray.length() - 1);
            int stockListNumber = (Integer) stockjson.get("number");
            int stock = (Integer) jsonobjScrapedSKU.get("stock");
            //比较HBase中的stocklist--number字段和Json数据中的stock字段的值
            if (stockListNumber != stock) {
                JSONObject stockjsonToList = new JSONObject();
                stockjsonToList.put("number", stock);
                stockjsonToList.put("time", jsonobjScrapedSKU.getString("lastseentime"));
                stockjsonArray.put(stockjsonToList);
                Put put = new Put(Bytes.toBytes(buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"))));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stock"), Bytes.toBytes(String.valueOf(stock)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stocklist"), Bytes.toBytes(String.valueOf(stockjsonArray)));
                hbaseClient.put(namespace, tableName, put);
            }
            //处理commentcountlist字段
            JSONArray commentcountjsonArray = new JSONArray(new String(commentcountlist));
            JSONObject commentcountjson = (JSONObject) commentcountjsonArray.get(commentcountjsonArray.length() - 1);
            int commentcountListNumber = (Integer) commentcountjson.get("number");
            int commentcount = (Integer) jsonobjScrapedSKU.get("commentcount");
            //比较HBase中的stocklist--number字段和Json数据中的stock字段的值
            if (commentcountListNumber != commentcount) {
                JSONObject commentcountjsonToList = new JSONObject();
                commentcountjsonToList.put("number", commentcount);
                commentcountjsonToList.put("time", jsonobjScrapedSKU.getString("lastseentime"));
                commentcountjsonArray.put(commentcountjsonToList);
                Put put = new Put(Bytes.toBytes(buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"))));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("commentcount"), Bytes.toBytes(String.valueOf(commentcount)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("commentcountlist"), Bytes.toBytes(String.valueOf(commentcountjsonArray)));
                hbaseClient.put(namespace, tableName, put);
            }
        }//不存在，继续下面的存入操作


        //将数据存入HBase中
        Put put = new Put(Bytes.toBytes(buildHBaseRowkeyFromURL(jsonobjScrapedSKU.getString("src_url"))));
        List<HbaseTableSchemas.ColumnFields> columnFieldsList = skuDataTable.columnFieldsList;
        for (HbaseTableSchemas.ColumnFields columnFields : columnFieldsList) {
            String columnName = columnFields.columnName;
            List<String> fields = columnFields.fields;
            for (String field : fields) {
                Object value = null;
                try {
                    if ("info".equals(columnName)) {
                        //获取columnfamily为"info"时，判断，当对应的field值为null时，则赋予 value 值为 "null"，为null则不存储。
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


    }

}
