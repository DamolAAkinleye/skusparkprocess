package cn.datapark.process.sku.util;

/**
 * Created by eason on 15/9/2.
 */
public class ConstantUtil  implements java.io.Serializable {

    public static class HBaseTable {
        //注：此处常量需跟hbase_skudata_schema.xml文件中配置一致
        //hbase skudata
        public static final String HBaseTable_SkuData_Namespace = "dpa";
        public static final String HBaseTable_SkuData_TableName = "skudata_0306";
        //判断sku是否存在，需要定位的列簇
        public static final String HBaseTable_Columnfamily = "info";
        //判断sku是否存在，需要定位的列名
        public static final String HBaseTable_Columnfield = "src_url";


        //hbase skucomment
//        public static final String HBaseTable_SkuComment_Namespace = "dpa";
//        public static final String HBaseTable_SkuComment_TableName = "skucommentest_0213";
    }
}
