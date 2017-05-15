package cn.datapark.process.sku.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 对HBase进行各种操作提供了方法
 * Created by eason on 16/1/20.
 */
public class HBaseClient implements java.io.Serializable {

    private static final Logger LOG = Logger.getLogger(HBaseClient.class);

    //map key的常量 rowkey ,列族,cell value
    public static final String ROWKEY = "rowkey";
    public static final String COLUMN_FAMILY = "columnfamily";
    public static final String COLUMN_NAME = "columnname";
    public static final String CELL_VALUE = "cellvalue";

    private Configuration hbaseConf = null;

    private static Connection conn = null;

    synchronized public void initClient(String zookeeperQuorum, String zookeeperPort, String isClusterDisctributed) {
        if (conn != null) {
            LOG.info("HBaseClient already load,close before reinit");
            return;
        }
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        hbaseConf.set("hbase.cluster.distributed", isClusterDisctributed);
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");//使用horton ambari安装的集群需要使用该设置
        try {
            conn = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        conn = null;
        LOG.info("HBase Connection close");
    }

    /**
     * 创建多列族表
     *
     * @param nameSpace
     * @param tableName
     * @param columnFamilys
     */
    public void createTable(String nameSpace, String tableName, List<String> columnFamilys) {


        Admin admin = null;
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        boolean isNameSpaceExist = false;
        try {
            NamespaceDescriptor npd = admin.getNamespaceDescriptor(nameSpace);
            isNameSpaceExist = true;
            LOG.info("Namespace " + nameSpace + " exists");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            if (!isNameSpaceExist) {
                LOG.info("creating namespace " + nameSpace);
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
                LOG.info("Namespace " + nameSpace + "create success!");
            }

            String tableNameWithNameSpace = nameSpace + ":" + tableName;

            TableName tableNameObj = TableName.valueOf(tableNameWithNameSpace);
            if (admin.tableExists(tableNameObj)) {
                LOG.error("Table " + tableNameWithNameSpace + " exists!");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableNameWithNameSpace);
                for (String columnFamily : columnFamilys) {
                    tableDesc.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(tableDesc);
                LOG.info("Create " + tableName + ":" + Arrays.asList(columnFamilys) + " success");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建单列族表
     *
     * @param nameSpace
     * @param tableName
     * @param columnFamily
     */
    public void createTable(String nameSpace, String tableName, String columnFamily) {


        Admin admin = null;
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        boolean isNameSpaceExist = false;
        try {
            NamespaceDescriptor npd = admin.getNamespaceDescriptor(nameSpace);
            isNameSpaceExist = true;
            LOG.info("Namespace " + nameSpace + " exists");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            if (!isNameSpaceExist) {
                LOG.info("creating namespace " + nameSpace);
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
                LOG.info("Namespace " + nameSpace + "create success!");
            }

            String tableNameWithNameSpace = nameSpace + ":" + tableName;

            TableName tableNameObj = TableName.valueOf(tableNameWithNameSpace);
            if (admin.tableExists(tableNameObj)) {
                LOG.error("Table " + tableNameWithNameSpace + " exists!");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableNameWithNameSpace);
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(tableDesc);
                LOG.info("Create " + tableName + ":" + columnFamily + " success");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, String columnFamily) throws IOException {
        createTable("default", tableName, columnFamily);
    }


    public boolean put(String nameSpace, String tableName, Put put) {

        boolean isPutSuc = false;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(nameSpace + ":" + tableName));
            table.put(put);
            isPutSuc = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isPutSuc;
    }

    public boolean put(String nameSpace, String tableName, String columnFamily, String colName, String value, String rowKey) {

        boolean isPutSuc = false;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(nameSpace + ":" + tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colName), Bytes.toBytes(value));
            table.put(put);
            isPutSuc = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isPutSuc;
    }


    public boolean batchPuts(String nameSpace, String tableName, List<Put> puts) {
        boolean isPutSuc = false;
        Table table = null;

        try {
            table = conn.getTable(TableName.valueOf(nameSpace + ":" + tableName));
            table.put(puts);
            isPutSuc = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return isPutSuc;
    }

    /**
     * 默认namespace为default
     *
     * @param tableName
     * @param valueList
     */
    public boolean batchPutMap(String tableName, List<Map<String, Object>> valueList) {
        return batchPutMap("default", tableName, valueList);
    }


    public boolean batchPutMap(String nameSpace, String tableName, List<Map<String, Object>> valueList) {

        List<Put> puts = new ArrayList<Put>();

        for (Map<String, Object> row : valueList) {
            Put put = new Put(row.get(ROWKEY).toString().getBytes());
            put.addColumn(Bytes.toBytes(row.get(COLUMN_FAMILY).toString()), Bytes.toBytes(row.get(COLUMN_NAME).toString()), Bytes.toBytes(row.get(CELL_VALUE).toString()));
            puts.add(put);
        }

        return batchPuts(nameSpace, tableName, puts);

    }


    public byte[] getCellValueByRowkey(String rowkey, String nameSpace, String tableName, String columnFamilyName, String ColumnName) {

        Table table = null;
        try {

            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
            Get get = new Get(rowkey.getBytes());
            get.addFamily(columnFamilyName.getBytes());
            Result result = table.get(get);

            if (result != null) {
                byte[] b = result.getValue(columnFamilyName.getBytes(), ColumnName.getBytes());
                return b;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

//    /**
//     * 根据正则获取符合条件的rowkey的数据
//     * 返回对应的rowkey
//     * @param scanPatten
//     * @param nameSpace
//     * @param tableName
//     * @return
//     */
//    public byte[] getRowkeybyScanCompareOp(String scanPatten, String nameSpace, String tableName) {
//        Table table = null;
//        try {
//            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
//
//            Scan scan = new Scan();
//            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(scanPatten));
//            scan.setFilter(filter);
//            ResultScanner scanner;
//            scanner = table.getScanner(scan);
//            for (Result res : scanner) {
//                return res.getRow();
//            }
//        } catch (Exception e){
//            e.fillInStackTrace();
//        }
//        finally {
//
//            try {
//                if (table != null)
//                    table.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        return null;
//    }

    /**
     * 获取HBase中所有Rowkey,并且放入Set集合中
     *
     * @param nameSpace
     * @param tableName
     * @return
     */
    public Set<String> getRowkeybyScan(String nameSpace, String tableName) {
        Set<String> set = new HashSet<String>();
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
            Scan scan = new Scan();
            ResultScanner scanner;
            scanner = table.getScanner(scan);
            for (Result res : scanner) {
                set.add(new String(res.getRow()));
            }
            return set;
        } catch (Exception e) {
            e.fillInStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 获取HBase中所有Rowkey,并且放入Set集合中
     *
     * @param nameSpace
     * @param tableName
     * @return
     */
    public List<JSONObject> getDatabyScan(String nameSpace, String tableName) {
        List<JSONObject> list = new ArrayList<JSONObject>();
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
            Scan scan = new Scan();
            ResultScanner scanner;
            scanner = table.getScanner(scan);
            for (Result res : scanner) {
                JSONObject json = new JSONObject();
                for (KeyValue keyValue : res.raw()) {
                    json.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
                }
                list.add(json);
            }
            return list;
        } catch (Exception e) {
            e.fillInStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }



    /**
     * 根据正则获取符合条件的rowkey的数据
     * 返回对应的rowkey
     *
     * @param scanPatten
     * @param nameSpace
     * @param tableName
     * @return
     */
    public List<JSONObject> getRowkeybyScanCompareOp(String scanPatten, String nameSpace, String tableName) {
        Table table = null;
        List<JSONObject> list = new ArrayList<JSONObject>();
        try {
            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
            Scan scan = new Scan();

            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(scanPatten));//.*20170215
            scan.setFilter(filter);
            ResultScanner scanner;
            scanner = table.getScanner(scan);
            for (Result r : scanner) {
                JSONObject json = new JSONObject();
                for (KeyValue keyValue : r.raw()) {
                    json.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
                }
                list.add(json);
            }
            return list;
        } catch (Exception e) {
            e.fillInStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 调用此方法  传入两个字符串时间，精确到毫秒
     * 内置方法将字符串时间转换成时间戳
     * @param minStamp
     * @param maxStamp
     * @param nameSpace
     * @param tableName
     * @return
     */
    public List<JSONObject> getbyScanTimeRange(String nameSpace, String tableName, String minStamp, String maxStamp) {
        Table table = null;
//        List<List<String>> listSum = new ArrayList<List<String>>();
//        List<String> list = new ArrayList<String>();
        List<JSONObject> list = new ArrayList<JSONObject>();
        try {
            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
            Scan scan = new Scan();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

            long min = formatter.parse(minStamp).getTime();
            long max = formatter.parse(maxStamp).getTime();
            System.out.println("mintime: "+ min);
            System.out.println("maxtime: "+ max);
            scan.setTimeRange(min,max);
            ResultScanner scanner;
            scanner = table.getScanner(scan);

            for (Result r : scanner) {
                //System.out.println("rowkey:" + new String(r.getRow()));
                //System.out.println(new String(String.valueOf(r)));
                JSONObject json = new JSONObject();
                for (KeyValue keyValue : r.raw()) {
                    //System.out.println(new String(keyValue.getFamily())+"--"+new String(keyValue.getQualifier()) + ":" + new String(keyValue.getValue()));
                    //System.out.println(new String(keyValue.getQualifier()) + ":" + new String(keyValue.getValue()));
                    json.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
                }
                list.add(json);
            }
            return list;
        } catch (Exception e) {
            e.fillInStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }




    public static void main(String arg[]) throws IOException {
        HBaseClient client = new HBaseClient();
        DataToHBase dh = new DataToHBase();

        client.initClient("192.168.31.63,192.168.31.61,192.168.31.62", "2181", "true");
//        client.createTable("dpa", "test", "cf0");
//        client.put("dpa", "test", "cf0", "aaaa", "222222222", "RRRRRRRRR123");
//        byte[] r=client.getCellValueByRowkey("cn.busines20160320234914http://cn.businessoffashion.com/2015/05/south-korea-climbing-the-ranks-of-asias-a-league-2.html","dpa", "articles", "d", "src_url");
//        byte[] r = client.getRowkeybyScanCompareOp(".*http://www.vogue.com.cn/people/movie/pic_151334b0e27cb092.html", "dpa", "skudatatest_0209");
//        byte[] r = client.getRowkeybyScanCompareOp(url, "dpa", "skudatatest_0209");

//        Map<String,List<String>> map = client.getRowkeybyScanCompareOp(".*20170214","dpa", "skudatatest_0214");
////        String result = new String(r);
////        System.out.println(result);
//        for (Map.Entry<String,List<String>> entry : map.entrySet()) {
//            System.out.println("key= " + entry.getKey());
//            System.out.println("value= " + entry.getValue());
//        }
        List<JSONObject> list = client.getRowkeybyScanCompareOp(".*20170216", "dpa", "skudatatest_0214");

        for (JSONObject attribute : list) {
            System.out.println(attribute);
        }
        client.close();
    }
}
