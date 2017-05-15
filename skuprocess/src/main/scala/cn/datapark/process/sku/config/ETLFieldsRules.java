package cn.datapark.process.sku.config;

import cn.datapark.process.sku.preprocess.FieldAliasProcessor;
import cn.datapark.process.sku.util.CharsetCodecUtil;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by eason on 15/10/31.
 * ETL 抽取转换配置
 * 配置项key，key的value为某个Field名
 * 配置项actions，actions的list为需要为该Field进行处理的动作，如中英文分离，大小写是否敏感，是否要去掉特殊字符等,按照config中出现的顺序执行，可以重复
 * 配置项values,values中的内容为按照action处理后的field的内容需要进行匹配的字符串，匹配到的field的alias内容填写对应的value
 */
public class ETLFieldsRules {

    private static final Logger LOG = Logger.getLogger(ETLFieldsRules.class);


    private HashMap<String, ETLFieldRule> config = new HashMap<String, ETLFieldRule>();

    public class ETLFieldRule {
        private String Key; //需要进行别名处理的字段  color,print.....
        private ArrayList<String> Actions = new ArrayList<String>();//别名处理规则列表  remove-escapechar  case-insensitive
        private HashSet<String> Values = new HashSet<String>(); //按照预处理规则进行预处理的别名配置
        private HashMap<String, String> RawValues = new HashMap<String, String>();//处理后的字段别名和未处理的字段别名对应表
        private HashMap<String, String> ALTValues = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> ENValues = new HashMap<String, String>();//英文别名对应表
        private HashMap<String, String> CNValues = new HashMap<String, String>();//中文别名对应表

        public String getKey() {
            return Key;
        }

        public int getActionsSize() {
            return Actions.size();
        }

        public String getActionAt(int i) {
            return Actions.get(i);
        }

        public Iterator<String> getValueIterator() {
            return Values.iterator();
        }

        public HashMap<String, String> getATLValues() {
            return ALTValues;
        }

        public HashMap<String, String> getENValues() {
            return ENValues;
        }

        public HashMap<String, String> getCNValues() {
            return CNValues;
        }

        public HashMap<String, String> getRawValues() {
            return RawValues;
        }


        public boolean containsValue(String value) {

            if (Values.contains(value)) {
                return true;
            } else if (CNValues.containsKey(value)) {
                return true;
            } else if (ENValues.containsKey(value)) {
                return true;
            }
            return false;
        }

        //key 需要处理的别名字段值
        //RawValue处理后的字段别名和未处理的字段别名对应表
        public String getRawValue(String key) {

            String rawValue = RawValues.get(key);
            if (rawValue == null) {
                rawValue = CNValues.get(key);
                if (rawValue == null) {
                    rawValue = ENValues.get(key);
                }
            }

            return rawValue;

        }

        //获取别名的替代名称
        public String getValueFromAtlValue(String value) {
            return ALTValues.get(value);
        }


        /**
         * Begin:王坤造:颜色匹配功能模块
         */

        //包含色字,且长度为2的HashMap
        private HashMap<String, String> SeValues2 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> SeValues3 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> SeValues4 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> SeValues5 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> SeValues6 = new HashMap<String, String>();//别名的替代名称对应表
        //没有色字,且长度为1的HashMap
        private HashMap<String, String> NoSeValues1 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> NoSeValues2 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> NoSeValues3 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> NoSeValues4 = new HashMap<String, String>();//别名的替代名称对应表
        private HashMap<String, String> NoSeValues5 = new HashMap<String, String>();//别名的替代名称对应表

        /**
         * 测试用的,从本地文件中初始化颜色替换规则到HashMap中
         */
        public void initMyColor() throws Exception {
            String sePath = "D:/LastColor";
            String noSePath = "D:/LastColorNo";
            String line = null;
            int index;
            HashMap<String, String> temp = new HashMap<String, String>();


            File dir = new File(sePath);
            File[] files = dir.listFiles();
            if (files.length > 0) {
                for (File file : files) {
                    BufferedReader bufr = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    while ((line = bufr.readLine()) != null) {
                        if (line.trim().length() > 0) {
                            temp.put(line, line);
                        }
                    }
                    bufr.close();
                    index = new Integer("" + file.getName().charAt(5));
                    switch (index) {
                        case 2:
                            SeValues2.putAll(temp);
                            break;
                        case 3:
                            SeValues3.putAll(temp);
                            break;
                        case 4:
                            SeValues4.putAll(temp);
                            break;
                        case 5:
                            SeValues5.putAll(temp);
                            break;
                        default:
                            SeValues6.putAll(temp);
                            break;
                    }
                    temp.clear();
                }
            }

            dir = new File(noSePath);
            files = dir.listFiles();
            if (files.length > 0) {
                for (File file : files) {
                    BufferedReader bufr = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    while ((line = bufr.readLine()) != null) {
                        if (line.trim().length() > 0) {
                            temp.put(line, line + "色");
                        }
                    }
                    bufr.close();
                    index = new Integer("" + file.getName().charAt(5));
                    switch (index) {
                        case 1:
                            NoSeValues1.putAll(temp);
                            break;
                        case 2:
                            NoSeValues2.putAll(temp);
                            break;
                        case 3:
                            NoSeValues3.putAll(temp);
                            break;
                        case 4:
                            NoSeValues4.putAll(temp);
                            break;
                        default:
                            NoSeValues5.putAll(temp);
                            break;
                    }
                    temp.clear();
                }
            }
        }

        /**
         * 王坤造的颜色替换方法;源字符串,源字符串是否包含'色'
         * //别名的替代名称对应表
         */
        public String getValueFromAtlValueNew(String sou, Boolean isSe) {
            String result = null;
            int length = sou.length();
            //要搜索的字符串中含有'色'
            if (isSe) {
                switch (length) {
                    case 2:
                        //直接进行完全匹配
                        result = matchPerfectly(SeValues2, sou);
                        if (result == null) {
                            //完全匹配不到,做其它操作
                            doOther(sou);
                        }
                        break;
                    case 3:
                        //先进行完全匹配
                        result = matchPerfectly(SeValues3, sou);
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(SeValues2, sou);
                            if (result == null) {
                                //包含匹配不到,做其它操作
                                doOther(sou);
                            }
                        }
                        break;
                    case 4:
                        //先进行完全匹配
                        result = matchPerfectly(SeValues4, sou);
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(SeValues3, sou);
                            if (result == null) {
                                //包含匹配不到,则进行包含匹配
                                result = matchContains(SeValues2, sou);
                                if (result == null) {
                                    //包含匹配不到,做其它操作
                                    doOther(sou);
                                }
                            }
                        }
                        break;
                    case 5:
                        //先进行完全匹配
                        result = matchPerfectly(SeValues5, sou);
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(SeValues4, sou);
                            if (result == null) {
                                //包含匹配不到,则进行包含匹配
                                result = matchContains(SeValues3, sou);
                                if (result == null) {
                                    //包含匹配不到,则进行包含匹配
                                    result = matchContains(SeValues2, sou);
                                    if (result == null) {
                                        //包含匹配不到,做其它操作
                                        doOther(sou);
                                    }
                                }
                            }
                        }
                        break;
                    default:
                        if (length > 6) {
                            //长度刚好大于6,先进行包含匹配
                            result = matchContains(SeValues6, sou);
                        } else {
                            //长度刚好等于6,先进行完全匹配
                            result = matchPerfectly(SeValues6, sou);
                        }
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(SeValues5, sou);
                            if (result == null) {
                                //包含匹配不到,则进行包含匹配
                                result = matchContains(SeValues4, sou);
                                if (result == null) {
                                    //包含匹配不到,则进行包含匹配
                                    result = matchContains(SeValues3, sou);
                                    if (result == null) {
                                        //包含匹配不到,则进行包含匹配
                                        result = matchContains(SeValues2, sou);
                                        if (result == null) {
                                            //包含匹配不到,做其它操作
                                            doOther(sou);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                }
            } else {//要搜索的字符串中没有'色'
                switch (length) {
                    case 1:
                        //直接进行完全匹配
                        result = matchPerfectly(NoSeValues1, sou);
                        if (result == null) {
                            //完全匹配不到,做其它操作
                            //doOther(sou);
                        }
                        break;
                    case 2:
                        //先进行完全匹配
                        result = matchPerfectly(NoSeValues2, sou);
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(NoSeValues1, sou);
                            if (result == null) {
                                //包含匹配不到,做其它操作
                                //doOther(sou);
                            }
                        }
                        break;
                    case 3:
                        //先进行完全匹配
                        result = matchPerfectly(NoSeValues3, sou);
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(NoSeValues2, sou);
                            if (result == null) {
                                //包含匹配不到,则进行包含匹配
                                result = matchContains(NoSeValues1, sou);
                                if (result == null) {
                                    //包含匹配不到,做其它操作
                                    //doOther(sou);
                                }
                            }
                        }
                        break;
                    case 4:
                        //先进行完全匹配
                        result = matchPerfectly(NoSeValues4, sou);
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(NoSeValues3, sou);
                            if (result == null) {
                                //包含匹配不到,则进行包含匹配
                                result = matchContains(NoSeValues2, sou);
                                if (result == null) {
                                    //包含匹配不到,则进行包含匹配
                                    result = matchContains(NoSeValues1, sou);
                                    if (result == null) {
                                        //包含匹配不到,做其它操作
                                        //doOther(sou);
                                    }
                                }
                            }
                        }
                        break;
                    default:
                        if (length > 6) {
                            //长度刚好大于5,先进行包含匹配
                            result = matchContains(NoSeValues5, sou);
                        } else {
                            //长度刚好等于5,先进行完全匹配
                            result = matchPerfectly(NoSeValues5, sou);
                        }
                        if (result == null) {
                            //完全匹配不到,则进行包含匹配
                            result = matchContains(NoSeValues4, sou);
                            if (result == null) {
                                //包含匹配不到,则进行包含匹配
                                result = matchContains(NoSeValues3, sou);
                                if (result == null) {
                                    //包含匹配不到,则进行包含匹配
                                    result = matchContains(NoSeValues2, sou);
                                    if (result == null) {
                                        //包含匹配不到,则进行包含匹配
                                        result = matchContains(NoSeValues1, sou);
                                        if (result == null) {
                                            //包含匹配不到,做其它操作
                                            //doOther(sou);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                }
            }
            return result;
        }

        /**
         * 判断字符串在HashMap的keyset中是否存在(key==sou),用于颜色匹配中
         */
        private String matchPerfectly(HashMap<String, String> hm, String sou) {
            Boolean b = hm.keySet().contains(sou);
            if (b) {
                return hm.get(sou);
            }
            return null;
        }

        /**
         * 判断字符串在HashMap的keyset中是否被包含(key.contains(cou)),用于颜色匹配中
         */
        private String matchContains(HashMap<String, String> hm, String sou) {
            for (String key : hm.keySet()) {
                if (sou.contains(key)) {
                    return hm.get(key);
                }
            }
            return null;
        }

        /**
         * 颜色匹配不到,要做的操作
         */
        private void doOther(String sou) {
//            LOG.info("-------------------------想干嘛？----------------"+sou);

//            String indexType = "nocolor";
//            String indexName = "untreatedcolor";
//            XContentBuilder doc = null;
//            Client client = ESUtil.getInstance().getESInstance();
//            try {
//                doc = jsonBuilder()
//                        .startObject()
//                        .field("colors", sou)
//                        .endObject();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            IndexResponse response = client.prepareIndex(indexName, indexType, null).setSource(doc).execute().actionGet();
        }


        /**
         * End:王坤造:颜色匹配功能模块
         * */
    }

    public class ETLFieldsConfigException extends Exception {
        public ETLFieldsConfigException(String message) {
            super(message);
        }
    }

    public Iterator<String> KeyIterator() {
        return config.keySet().iterator();
    }

    public ETLFieldRule getETLFieldConfigByKey(String key) {
        return config.get(key);
    }


    /**
     * 从XML文件中读取字段ETL处理规则表
     *
     * @param file 文件名
     * @throws ETLFieldsConfigException 字段别名处理异常
     */
    public void readFromXMLFile(String file) throws ETLFieldsConfigException {
        InputStreamReader in = null;

        try {
            //从xml文件中读取
            XMLConfiguration configfile = new XMLConfiguration();
            in = new InputStreamReader(ETLFieldRulesUtil.class.getClassLoader().getResourceAsStream((file)));

            byte[] unicodebytes = IOUtils.toByteArray(in);


            String utf8Config = CharsetCodecUtil.unicodeToUtf8(new String(unicodebytes));
            configfile.load(new ByteArrayInputStream(utf8Config.getBytes()));


            //List出 field中的内容
            List<HierarchicalConfiguration> fields = configfile.configurationsAt("field");

            for (HierarchicalConfiguration field : fields) {
                //生成ETLFieldConfig实例
                String key = field.getString("key").trim();
                ETLFieldRule efc = new ETLFieldRule();
                efc.Key = key;
                //遍历读取字段处理规则列表
                SubnodeConfiguration actions = field.configurationAt("actions");
                String[] action = actions.getStringArray("action");
                for (int i = 0; i < action.length; i++) {
                    efc.Actions.add(action[i].toLowerCase().trim());
                }


                //遍历读取字段别名配置内容列表
                List<HierarchicalConfiguration> valueList = field.configurationsAt("values.value");
                for (HierarchicalConfiguration hc : valueList) {

                    ConfigurationNode node = hc.getRootNode();
                    //处理后的字段别名和未处理的字段别名对应表
                    String rawValue = (String) node.getValue();
                    //读取字段别名替代名称子列表
                    List<ConfigurationNode> attrList = node.getAttributes("altvalues");
                    if (rawValue == null) {
                        LOG.error("Read xml config from file failed, cause: empty <value></value>");
                        throw new ETLFieldsConfigException("invalidate xml config file");
                    }

                    rawValue = rawValue.trim();
                    if ((attrList != null) && (attrList.size() > 0)) {
                        for (ConfigurationNode cn : attrList) {
                            String v = cn.getValue().toString().trim();
                            efc.ALTValues.put(v, rawValue);
                        }
                    }

                    //按照配置的别名处理规则对别名内容进行预处理，方便后期进行快速比较
                    String toMatchValue = rawValue;
                    if (efc.Actions.contains(FieldAliasProcessor.Action_Remove_EscapeChar)) {
                        //去除别名配置中的特殊符号，如空格，括号等
                        toMatchValue = toMatchValue.replaceAll(FieldAliasProcessor.EscapeCharacterRegEx, "");
                    }
                    if (efc.Actions.contains(FieldAliasProcessor.Action_Case_Insensitive)) {
                        //大小写不敏感的字段将别名配置内容小写
                        toMatchValue = toMatchValue.toLowerCase();
                    }

                    String fieldValueCN = null;
                    String fieldValueEN = null;
                    Matcher mEN = Pattern.compile(FieldAliasProcessor.RegexEN).matcher(toMatchValue);
                    Matcher mCN = Pattern.compile(FieldAliasProcessor.RegexCN).matcher(toMatchValue);

                    if (mCN.find()) {
                        fieldValueCN = mCN.group(1);
                    }
                    if (mEN.find()) {
                        fieldValueEN = mEN.group(1);
                    }

                    //中文别名对和别名匹配表
                    if (fieldValueCN != null) {
                        efc.CNValues.put(fieldValueCN.trim(), rawValue.trim());
                    }
                    //英文别名和别名匹配表
                    if (fieldValueEN != null) {
                        efc.ENValues.put(fieldValueEN.trim(), rawValue.trim());
                    }

                    //将修改后的内容和原始内容匹配i存入hashmap中，一遍别名处理最后使用原始别名内容作为输出
                    efc.RawValues.put(toMatchValue.trim(), rawValue.trim());
                    efc.Values.add(toMatchValue.trim());
                }

                //初始化颜色替换字典
                if (key.equals("color")) {
                    int index;
                    Set<String> keySet = efc.ALTValues.keySet();
                    for (String keyStr : keySet) {
                        index = keyStr.length();
                        if (keyStr.contains("色")) {
                            switch (index) {
                                case 2:
                                    efc.SeValues2.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                case 3:
                                    efc.SeValues3.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                case 4:
                                    efc.SeValues4.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                case 5:
                                    efc.SeValues5.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                default:
                                    efc.SeValues6.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                            }
                        } else {
                            switch (index) {
                                case 1:
                                    efc.NoSeValues1.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                case 2:
                                    efc.NoSeValues2.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                case 3:
                                    efc.NoSeValues3.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                case 4:
                                    efc.NoSeValues4.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                                default:
                                    efc.NoSeValues5.put(keyStr, efc.ALTValues.get(keyStr));
                                    break;
                            }
                        }
                    }
                }

                LOG.info("RawValues:" + efc.RawValues.toString());
                LOG.info("Values:" + efc.Values.toString());
                config.put(key, efc);
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

    public static void main(String args[]) {
        ETLFieldsRules efr = new ETLFieldsRules();
        try {
            ETLFieldsRules.ETLFieldRule fieldRule = efr.new ETLFieldRule();
            fieldRule.initMyColor();

            efr.readFromXMLFile("etlfieldrules.xml");
            //ETLFieldsRules efr = ETLFieldRulesUtil.getEtlFieldsRules();
            Iterator<String> it = efr.KeyIterator();
            while (it.hasNext()) {
                Object o = it.next();
                System.out.println(o);
            }
        } catch (Exception e) {

        }
        System.out.println(123);
    }
}
