package cn.datapark.process.sku.util;

import cn.datapark.process.sku.config.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/11.
 * esconfig，kafkaconfig，esmapping的配置管理集合
 */
@Service
public class DistributedConfigCollection implements java.io.Serializable {

    private static final Logger LOG = Logger.getLogger(DistributedConfigCollection.class);

//    @Autowired
//    private DPKafkaConfig dpKafkaConfig;

    @Autowired
    private HBaseConfig hBaseConfig;

    private HbaseTableSchemas hbaseTableSchemas = new HbaseTableSchemas();

    public DistributedConfigCollection() {

    }

//    public DPKafkaConfig getDPKafkaConfig() {
//        return dpKafkaConfig;
//    }

    public HBaseConfig getHBaseConfig() {
        return hBaseConfig;
    }


    public HbaseTableSchemas getHbaseTableSchemas() {
        return hbaseTableSchemas;
    }


    public void initHbaseTable(String resourcePath) throws ConfigurationException {
        hbaseTableSchemas.init(resourcePath);
    }

}
