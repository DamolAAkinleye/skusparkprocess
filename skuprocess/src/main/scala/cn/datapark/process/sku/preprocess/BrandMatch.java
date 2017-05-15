package cn.datapark.process.sku.preprocess;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 京东全球购数据，brand值为空，从name值中提取品牌名，并且赋给brand
 * Created by cluster on 2017/1/21.
 */
public class BrandMatch {
    private static XMLConfiguration mappingsConfig = null;

    //储存catindexmapping.xml中每个mappings中mapping的EN  ENalias CN
    private static HashSet<BNMapping> BNMappings = new HashSet<BNMapping>();

    //存放英文名，英文别名，中文名
    private static class BNMapping {
        public String EN;
        public String ENalias;
        public String CN;
        public String brandname;

    }

    /**
     * 实例化配置文件，获取内容
     *
     * @param resourcePath
     * @throws ConfigurationException
     */
    public static void init(String resourcePath) throws ConfigurationException {
        mappingsConfig = new XMLConfiguration(BrandMatch.class.getClassLoader().getResource((resourcePath)));
        //遍历读取所有category 和 indextype的映射
        List<HierarchicalConfiguration> maps = mappingsConfig.configurationsAt("mappings.mapping");
        for (HierarchicalConfiguration map : maps) {
            BNMapping bim = new BNMapping();
            bim.EN = map.getString("EN");
            bim.ENalias = map.getString("ENalias");
            bim.CN = map.getString("CN");
            bim.brandname = map.getString("brandname");
            BNMappings.add(bim);
        }
    }

    /**
     * 根据输入的name进行品牌的匹配
     * @param str
     * @return
     */
    /**
     * 根据输入的name进行品牌的匹配
     * 将所有符合条件的EN放入数组中，并且将数组中长度最大的元素返回
     * 根据：长度最大也就是匹配度最大！
     *
     * @param str
     * @return
     */
    public static String getBrandName_cec(String str) {
        Pattern pattern;
        Matcher matcher;
        Set<String> result = new HashSet<String>();// 目的是：相同的字符串只返回一个。。。 不重复元素
        Iterator it = BNMappings.iterator();
        String[] resultStr = null;
        while (it.hasNext()) {
            BNMapping bim = (BNMapping) it.next();
            if ((bim.CN).contains("null")) {
                //ENalias也为空的时候，采用EN进行匹配
//                pattern = Pattern.compile("\\b"+bim.EN+"\\b",Pattern.CASE_INSENSITIVE);
                pattern = Pattern.compile(bim.EN, Pattern.CASE_INSENSITIVE);
                matcher = pattern.matcher(str);
                if (matcher.find()) {
                    result.add(bim.brandname);
                }
                //当CN不为空，开始CN匹配
            } else {
//                pattern = Pattern.compile("\\b"+bim.CN.replaceAll(" ","")+"\\b",Pattern.CASE_INSENSITIVE);
                pattern = Pattern.compile(bim.CN.replaceAll(" ", ""), Pattern.CASE_INSENSITIVE);
                matcher = pattern.matcher(str);
                if (matcher.find()) {
                    result.add(bim.brandname);
                }
                //CN没有匹配到
                //如果CN没有匹配到的时候，采用EN进行匹配
                pattern = Pattern.compile(bim.EN, Pattern.CASE_INSENSITIVE);
                matcher = pattern.matcher(str);
                if (matcher.find())
                    result.add(bim.brandname);
            }
        }
        resultStr = new String[result.size()];
        // 将Set result转化为String[] resultStr
        if (resultStr.length > 0) {
            String[] array = result.toArray(resultStr);
            int index = 0;
            for (int i = 0; i < array.length; i++) {
                if (array[i].length() > array[index].length())
                    index = i;
            }
            String brandname = array[index];
            return brandname;
        } else
            return str;
    }

    /**
     * 将分割好的值进行中英文分离
     *
     * @param str
     * @return
     */
    public static String getRegString(String str) {
        String str1 = "";
        String str2 = "";
        // 英文组合
        Pattern p = Pattern.compile("[a-zA-Z0-9]+");
        Matcher m = p.matcher(str);
        while (m.find()) {
            str1 += m.group() + " ";
        }
        // 非英文组合
        p = Pattern.compile("[^a-zA-Z]+");
        m = p.matcher(str);
        while (m.find()) {
            str2 += m.group() + " ";
        }
        String regstr = str1 + str2;
        return regstr;
    }


    public static void main(String[] args) {
        BrandMatch bm = new BrandMatch();
        try {
            bm.init("brandmapping.xml");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        //根据某些关键字去截取  eg: 男，女，色，童..防止后面的型号颜色等信息影响匹配
        String name = "";
        String[] sp = name.split("[男女童色]");
        String namesp = sp[0].replaceAll("[·/()（）]", "");
        //将中英文分开
        String regstr = bm.getRegString(namesp);
        String brandname = bm.getBrandName_cec(regstr);
//        System.out.println("前："+regstr+"       后："+brandname);
    }

}
