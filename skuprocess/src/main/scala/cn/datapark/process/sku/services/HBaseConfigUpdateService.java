package cn.datapark.process.sku.services;

import cn.datapark.process.sku.config.HBaseConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by thinkpad on 2016/11/28.
 */
public class HBaseConfigUpdateService implements InitializingBean, DisposableBean {

    private static final Logger LOG = Logger.getLogger(KafkaConfigUpdateService.class);

    /**
     * 分布式配置
     */
    @Autowired
    private HBaseConfig hBaseConfig;

    /**
     * 关闭
     */
    public void destroy() throws Exception {
        LOG.info("destroy ==> ");
    }

    /**
     * 进行连接
     */
    public void afterPropertiesSet() throws Exception {

        LOG.info("connect ==> ");

    }

    /**
     * 后台更改值
     */
    public void changeConfig() {

        LOG.info("hbaseConfig changing to:");

        LOG.info("Quorum:" + hBaseConfig.getQuorum());
        LOG.info("port:" + hBaseConfig.getPort());

        LOG.info("change ok.");
    }
}
