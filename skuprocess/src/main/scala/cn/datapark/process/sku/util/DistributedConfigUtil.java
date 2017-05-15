package cn.datapark.process.sku.util;

import com.baidu.disconf.client.utils.SpringContextUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by eason on 15/9/12.
 * 单例模式管理配置项
 */
public class DistributedConfigUtil implements java.io.Serializable {

    private static final Logger LOG = Logger.getLogger(DistributedConfigUtil.class);

    private static DistributedConfigCollection distributedConfigCollection;

    private static DistributedConfigUtil instance = new DistributedConfigUtil();

    private static boolean isCollectionInit = false;

    private static String[] fn = null;

    private DistributedConfigUtil() {

        initContext();


    }

    // 初始化spring文档
    private static void contextInitialized() {

        fn = new String[]{"applicationContext.xml"};
    }

    private void initContext() {
        if (isCollectionInit) {
            return;
        }
        contextInitialized();

        new ClassPathXmlApplicationContext(fn);

        if (SpringContextUtil.getApplicationContext() == null) {
            LOG.error("-----------spring context is null------------");
            return;
        }
        distributedConfigCollection = (DistributedConfigCollection) SpringContextUtil.getBean(DistributedConfigCollection.class);

        try {

            distributedConfigCollection.initHbaseTable(distributedConfigCollection.getHBaseConfig().getSkuDateSchemaFile());

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        isCollectionInit = true;

    }

    public void destroy() {
        ((ClassPathXmlApplicationContext) SpringContextUtil.getApplicationContext()).close();
        isCollectionInit = false;
    }

    public static DistributedConfigUtil getInstance() {
        return instance;
    }

    public DistributedConfigCollection getDistributedConfigCollection() {
        return distributedConfigCollection;
    }


}
