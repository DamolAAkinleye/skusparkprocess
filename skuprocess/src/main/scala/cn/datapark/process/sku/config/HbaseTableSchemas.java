package cn.datapark.process.sku.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import java.util.*;

/**
 * Created by thinkpad on 2016/12/1.
 */
public class HbaseTableSchemas implements java.io.Serializable {
    private XMLConfiguration mappingsConfig = null;
    private List<Table> tableList = new ArrayList<Table>();
    private Map<String, ColumnFields> columnFieldsMap = new HashMap<String, ColumnFields>();//ColumnFields对象

    public class Table implements java.io.Serializable {
        public String namespace;//命名空间指对一组表的逻辑分组
        public String tableName;//表名
        public List<ColumnFields> columnFieldsList;//ColumnFields（columnName列簇，fields列名）

        public List<String> getColumnFaimlyList() {
            List<String> list = new ArrayList<String>();
            for (ColumnFields columnFields : columnFieldsList) {
                //循环将列簇添加到list中
                list.add(columnFields.columnName);
            }
            return list;
        }
    }

    public class ColumnFields implements java.io.Serializable {
        public String columnName;//列簇
        public List<String> fields;//列名
    }

    /**
     * 实例化HBasescgema配置，获取配置文件中的配置项
     *
     * @param resoucePath
     * @throws ConfigurationException
     */
    public void init(String resoucePath) throws ConfigurationException {
        mappingsConfig = new XMLConfiguration(HbaseTableSchemas.class.getClassLoader().getResource(resoucePath));
        List<HierarchicalConfiguration> tables = mappingsConfig.configurationsAt("table");
        for (HierarchicalConfiguration table : tables) {
            String namespace = table.getString("namespace");
            String tableName = table.getString("tablename");
            Table tabletmp = new Table();
            tabletmp.namespace = namespace;
            tabletmp.tableName = tableName;

            //获取columnfamilyname fields
            List<HierarchicalConfiguration> columnfamilys = table.configurationsAt("columnfamilys.columnfamily");
            List<ColumnFields> columnFieldsList = new ArrayList<ColumnFields>();
            for (HierarchicalConfiguration hc : columnfamilys) {

                String columnName = hc.getString("columnfamilyname");
                //一个columnfamilyname对应多个field，放入数组中
                String[] fieldArray = hc.getStringArray("fields.field");
                ColumnFields columnFields = new ColumnFields();
                columnFields.columnName = columnName;
                //将数组转换成List
                columnFields.fields = Arrays.asList(fieldArray);
                ;
                columnFieldsList.add(columnFields);
            }
            tabletmp.columnFieldsList = columnFieldsList;
            tableList.add(tabletmp);
        }
    }

    /**
     * 返回tableList
     *
     * @return
     */
    public List<Table> getTableList() {
        return tableList;
    }


    public static void main(String[] args) {
        HbaseTableSchemas hbaseTableSchemas = new HbaseTableSchemas();
        try {
            //读取的配置文件是 src/main/resources目录下的
            hbaseTableSchemas.init("hbase_skudata_schema.xml");
            List<Table> tableListTmp = hbaseTableSchemas.getTableList();
            for (Table table : tableListTmp) {
                System.out.println("namespace:" + table.namespace);
                System.out.println("tableName:" + table.tableName);
                for (ColumnFields columnFields : table.columnFieldsList) {
                    System.out.println("**columnName:" + columnFields.columnName);
                    for (String field : columnFields.fields) {
                        System.out.println("----field:" + field);
                    }
                }
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

}
