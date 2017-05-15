package cn.datapark.process.sku.config;

import org.apache.log4j.Logger;

/**
 * Created by eason on 15/10/31.
 * 单例工具，用户帮助和控制访问ETLFieldsConfig
 */
public class ETLFieldRulesUtil {
    private static final Logger LOG = Logger.getLogger(ETLFieldRulesUtil.class);

    private static ETLFieldsRules etlFieldsRules = new ETLFieldsRules();

    private static volatile boolean isInit = false;

    public static final String MysqlDriver = "com.mysql.jdbc.Driver";//支持从mysql数据中读取
    public static final String FileDriver = "file";//支持从xml文件中读取

    private ETLFieldRulesUtil() {

    }

    /**
     * 初始化 ETLFieldsRules,synchronized保证在storm中多线程访问的互斥，保证只被初始化一次
     *
     * @param URL    配置内容源地址，如果是mysql数据库，则传参为完整的数据库连接url
     * @param driver 配置内容源的类型，mysql数据库或文件
     * @return
     */
    synchronized public static boolean init(String URL, String driver) {

        LOG.info("ETLFieldsRules initializing......");
        if (isInit) {
            LOG.info("ETLFieldsRules already init......");
            return false;
        }


        LOG.info("Init Driver: " + driver + " url: " + URL);

        if (driver.equalsIgnoreCase(MysqlDriver)) {
            LOG.error("MysqlDriver not support now!!!!!!");

        } else if (driver.equalsIgnoreCase(FileDriver)) {
            try {
                etlFieldsRules.readFromXMLFile(URL);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else {
            LOG.error("ETLFieldsRules init: not support driver: " + driver);
            return false;
        }


        isInit = true;
        LOG.info("ETLFieldsRules init OK");
        return isInit;

    }

    public static ETLFieldsRules getEtlFieldsRules() {
        return etlFieldsRules;
    }


}
