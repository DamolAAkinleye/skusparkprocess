package cn.datapark.process.sku.util;


import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by thinkpad on 2016/12/19.
 */
public class DataFilters {

    public static List<String> nameFilters = new ArrayList<String>();

    public static List<NamePriceFilter> namePriceFilters = new ArrayList<NamePriceFilter>();

    public static class NamePriceFilter {
        public String name;
        public int price;
    }

    synchronized public static void readFromXmlFile(String resourcePath) {
        InputStreamReader in = null;

        try {
            //从xml文件中读取
            XMLConfiguration configfile = new XMLConfiguration();
            in = new InputStreamReader(DataFilters.class.getClassLoader().getResourceAsStream((resourcePath)));

            byte[] unicodebytes = IOUtils.toByteArray(in);


            String utf8Config = CharsetCodecUtil.unicodeToUtf8(new String(unicodebytes));
            configfile.load(new ByteArrayInputStream(utf8Config.getBytes()));

            SubnodeConfiguration filter = configfile.configurationAt("filters");

            String[] names = filter.getStringArray("namefilter.name");
            nameFilters = Arrays.asList(names);

            List<HierarchicalConfiguration> namepriceList = filter.configurationsAt("namepricefilter.nameprice");
            for (HierarchicalConfiguration node : namepriceList) {
                NamePriceFilter nameprice = new NamePriceFilter();
                nameprice.name = node.getString("name");
                nameprice.price = node.getInt("price");
                namePriceFilters.add(nameprice);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        DataFilters.readFromXmlFile("datafilter.xml");
    }
}
