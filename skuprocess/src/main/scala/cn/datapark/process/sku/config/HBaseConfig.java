package cn.datapark.process.sku.config;

import cn.datapark.process.sku.util.DistributedConfigUtil;
import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by thinkpad on 2016/11/28.
 */
@Service
@Scope("singleton")
@DisconfFile(filename = "hbase.properties")
public class HBaseConfig implements java.io.Serializable {

    private String Quorum;
    private String Port;
    private String IsDistributed;
    private String SkuDateSchemaFile;

    private String MysqlDriver;
    private String Username;
    private String Password;

    @DisconfFileItem(name = "hbase.zookeeper.quorum", associateField = "Quorum")
    public String getQuorum() {
        return Quorum;
    }

    public void setQuorum(String quorum) {
        Quorum = quorum;
    }

    @DisconfFileItem(name = "hbase.zookeeper.port", associateField = "Port")
    public String getPort() {
        return Port;
    }

    public void setPort(String port) {
        Port = port;
    }

    @DisconfFileItem(name = "hbase.zookeeper.isdistributed", associateField = "IsDistributed")
    public String getIsDistributed() {
        return IsDistributed;
    }

    public void setIsDistributed(String isDistributed) {
        IsDistributed = isDistributed;
    }

    @DisconfFileItem(name = "hbase.skudata.table.schema.file", associateField = "SkuDateSchemaFile")
    public String getSkuDateSchemaFile() {
        return SkuDateSchemaFile;
    }

    public void setSkuDateSchemaFile(String skuDateSchemaFile) {
        SkuDateSchemaFile = skuDateSchemaFile;
    }

    @DisconfFileItem(name = "mysql.driver", associateField = "MysqlDriver")
    public String getMysqlDriver() {
        return MysqlDriver;
    }

    public void setMysqlDriver(String mysqlDriver) {
        MysqlDriver = mysqlDriver;
    }

    @DisconfFileItem(name = "mysql.user", associateField = "Username")
    public String getUsername() {
        return Username;
    }

    public void setUsername(String username) {
        Username = username;
    }

    @DisconfFileItem(name = "mysql.password", associateField = "Password")
    public String getPassword() {
        return Password;
    }

    public void setPassword(String password) {
        Password = password;
    }


    public static void main(String args[]){
        HBaseConfig hBaseConfig = DistributedConfigUtil.getInstance().getDistributedConfigCollection().getHBaseConfig();
        System.out.println(hBaseConfig.getSkuDateSchemaFile());
    }
}
