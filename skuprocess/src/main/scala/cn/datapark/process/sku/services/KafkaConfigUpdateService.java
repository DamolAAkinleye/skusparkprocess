package cn.datapark.process.sku.services;

import cn.datapark.process.sku.config.DPKafkaConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/12.
 */
@Service
@Scope("singleton")
public class KafkaConfigUpdateService implements InitializingBean, DisposableBean {

    private static final Logger LOG = Logger.getLogger(KafkaConfigUpdateService.class);

    /**
     * 分布式配置
     */
    @Autowired
    private DPKafkaConfig kafkaConfig;

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

        LOG.info("kafkaConfig changing to:");

        LOG.info("Topic:" + kafkaConfig.getTopic());
        LOG.info("ConsumerRoot:" + kafkaConfig.getConsumerRoot());
        LOG.info("Zookeeper IP:" + kafkaConfig.getZookeeperServer());


        LOG.info("change ok.");
    }
}

