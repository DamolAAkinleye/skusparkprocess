package cn.datapark.process.sku.preprocess;

import cn.datapark.process.sku.util.DataFilters;
import cn.datapark.process.sku.config.ETLFieldRulesUtil;
import cn.datapark.process.sku.config.ETLFieldsRules;
import org.apache.log4j.Logger;
import net.sf.json.JSONObject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by eason on 15/10/31.
 * 根据配置文件中对Field的处理规则，进行数据预处理，将字段内容归一化
 */
public class FieldAliasProcessor {

    private static final Logger LOG = Logger.getLogger(FieldAliasProcessor.class);

    public static final String Action_To_Half_Width = "to-halfwidth";//转半角
    public static final String Action_Remove_EscapeChar = "remove-escapechar";//清除特殊字符
    public static final String Action_Case_Insensitive = "case-insensitive";//大小写不敏感，转小写处理
    public static final String Action_Split_EN = "split-en"; //将英文拆出比较英文部分，请放到配置项最后
    public static final String EscapeCharacterRegEx = "[`~!@#$%^&*()+\\-=|{}':;'°,\\[\\].<>/? ~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？· 　 ]";
    public static final String NumberAndLetterRegEx = "[0-9A-Za-z]";
    public static final String RegexEN = "([a-zA-Z0-9]+)";
    public static final String RegexCN = "([\u4e00-\u9fa5]+)";

    /**
     * 匹配查找统一别名(使用matchAliasNameNew进行替换,里面加了color的替换方式)
     * <p>
     * 如果是 brand，print,material字段，则进入这个方法
     *
     * @param fieldKey       需要查找的字段
     * @param rawFieldValue  需要匹配的原始字段内容
     * @param etlFieldsRules ETL规则配置
     * @return 如果字段和字段内容在etl规则中能够匹配到，则返回规定的统一别名
     */
    private static String matchAliasName(String fieldKey, String rawFieldValue, ETLFieldsRules etlFieldsRules) {

        String fieldAlias = null;
        ETLFieldsRules.ETLFieldRule fieldRule = etlFieldsRules.getETLFieldConfigByKey(fieldKey);
        if (fieldRule == null) {
            LOG.error("etlFieldsRules Null");
            return null;
        }
        //yfh add start
        //首先 判断fieldKey是否是material,并且含有 "%" 的情况 ，如果含有则先进入这个处理环节
        if (fieldKey.equals("material") && rawFieldValue.contains("%")) {//add by yfh start
            //加此处逻辑，因材质字段值中有 按百分比 组成的，比如：聚酯纤维95% 聚氨酯弹性纤维(氨纶)5%
            String[] splitMate = rawFieldValue.split("%");
            //取第一个，并去除数字和.
            rawFieldValue = splitMate[0].trim().replaceAll("\\d+", "").replace(".", "");
            //atlValues 别名的替代名称对应表
            HashMap<String, String> atlValues = fieldRule.getATLValues();
            Set<String> keys = atlValues.keySet();
            for (String key : keys) {
                if (key != null && key.length() > 0) {
                    if (rawFieldValue.contains(key)) {
                        rawFieldValue = key;
                        break;
                    }
                }
            }
        }
        //add by yfh end
        //如果fieldKey是 brand,color,print 则直接执行
        String fieldValue = rawFieldValue;
        //求出
        fieldValue = fieldValue.trim();
        String fieldValueEN = null;
        String fieldValueCN = null;

        //根据每个字段中的规则列表对字段内容进行预处理后在进行匹配，按照配置文件中的规则出现顺序处理
        for (int i = 0; i < fieldRule.getActionsSize(); i++) {
            String action = fieldRule.getActionAt(i);
            if (action.equalsIgnoreCase(Action_To_Half_Width)) {
                //全角转半角处理
            } else if (action.equalsIgnoreCase(Action_Remove_EscapeChar)) {
                //清除特殊字符
                Pattern p = Pattern.compile(EscapeCharacterRegEx);
                Matcher m = p.matcher(fieldValue);
                fieldValue = m.replaceAll("");
            } else if (action.equalsIgnoreCase(Action_Case_Insensitive)) {
                //比较的内容大消息不敏感，统一转为小写处理
                fieldValue = fieldValue.toLowerCase();
            } else if (action.equalsIgnoreCase(Action_Split_EN)) {
                //将字段中的中英文分开，供后面对中英文分别比较提供内容，该配置在配置文件中最好放在最后
                Matcher mEN = Pattern.compile(RegexEN).matcher(fieldValue);
                Matcher mCN = Pattern.compile(RegexCN).matcher(fieldValue);
                if (mCN.find()) {
                    fieldValueCN = mCN.group(1);
                }
                if (mEN.find()) {
                    fieldValueEN = mEN.group(1);
                }
            }
        }

        //参考正向最大匹配原则
        //containsValue():
        if (fieldRule.containsValue(rawFieldValue)) {
            //如果原始字段内容在匹配表中存在，则返回原始字段为别名
            fieldAlias = fieldRule.getRawValue(rawFieldValue);

        } else if (fieldRule.containsValue(fieldValue)) {
            //经过处理，但未拆分中英文的内容进行比较，如果存在于匹配表中，则返回该内容作为别名
            fieldAlias = fieldRule.getRawValue(fieldValue);

        } else if (fieldRule.containsValue(fieldValueCN)) {

            //中文在别名配置表中
            fieldAlias = fieldRule.getRawValue(fieldValueCN);

        } else if (fieldRule.containsValue(fieldValueEN)) {

            //英文在别名表中
            fieldAlias = fieldRule.getRawValue(fieldValueEN);

        } else if (fieldRule.getValueFromAtlValue(rawFieldValue) != null) {
            //比较原始字段内容是否在别名表的替代名称中出现，如果有，则使用别名替代名称对应的正式名称作为别名
            /**此处为一个bug，更新如下代码---update by yfh 2016-12-15 14:03:22
             fieldAlias = fieldRule.getValueFromAtlValue(fieldValue);
             **/
            fieldAlias = fieldRule.getValueFromAtlValue(rawFieldValue);

        } else if (fieldRule.getValueFromAtlValue(fieldValue) != null) {
            //比较处理后但未拆分成中英文的内容是否在别名表的替代名称中出现，如果有，则使用别名替代名称对应的正式名称作为别名

            fieldAlias = fieldRule.getValueFromAtlValue(fieldValue);

        } else if (fieldRule.getValueFromAtlValue(fieldValueCN) != null) {
            //比较纯中文字段内容是否在别名表的替代名称中出现，如果有，则使用别名替代名称对应的正式名称作为别名

            fieldAlias = fieldRule.getValueFromAtlValue(fieldValueCN);

        } else if (fieldRule.getValueFromAtlValue(fieldValueEN) != null) {
            //比较纯英文内容是否在别名表的替代名称中出现，如果有，则使用别名替代名称对应的正式名称作为别名
            fieldAlias = fieldRule.getValueFromAtlValue(fieldValueEN);

        } else {

            HashMap<String, String> atlValues = fieldRule.getATLValues();
            Set<String> keys = atlValues.keySet();
            for (String key : keys) {
                if (rawFieldValue.contains(key)) {
                    fieldAlias = atlValues.get(key);
                }
            }

        }

        //没有别名，则用原始字段内容作为别名。
        //没有别名的情况可能为：
        //1. 别名尚未被定义
        //2. 新出现的字段内容
        if (fieldAlias == null) {
            if (fieldKey.equals("material") || fieldKey.equals("style")) {
                fieldAlias = "其他";
            } else if (fieldKey.equals("print")) {
                fieldAlias = "其他图案";
            } else if (fieldKey.equals("brand")) {
                fieldAlias = null;
            } else {
                fieldAlias = rawFieldValue;
            }
        }
        LOG.debug("FieldKey: " + fieldKey + "RawFieldValue: " + rawFieldValue + "FieldAlias: " + fieldAlias);

//        return fieldAlias.trim();
        return fieldAlias;

    }

    /**
     * 匹配查找统一别名
     * 首先处理 color 字段内容
     *
     * @param fieldKey       需要查找的字段
     * @param rawFieldValue  需要匹配的原始字段内容
     * @param etlFieldsRules ETL规则配置
     * @return 如果字段和字段内容在etl规则中能够匹配到，则返回规定的统一别名
     */
    public static Object matchAliasNameNew(String fieldKey, String rawFieldValue, ETLFieldsRules etlFieldsRules) {
        String fieldAlias = null;
        ETLFieldsRules.ETLFieldRule fieldRule = etlFieldsRules.getETLFieldConfigByKey(fieldKey);
        if (fieldRule == null) {
            LOG.error("etlFieldsRules Null");
            return null;
        }
        rawFieldValue = rawFieldValue.trim();
        //这个是针对color属性进行特殊替换其它的还是按原来的方式进行替换
        if (fieldKey.equalsIgnoreCase("color")) {
            //将color字段分割成字符串数组
            String[] arr = rawFieldValue.split(",");
            if (arr.length > 0) {
                StringBuilder sb = new StringBuilder();
                HashSet<String> colorHS = new HashSet<String>();
                for (String str : arr) {
                    if (str != null && str.length() > 0) {
                        //将特殊字符串替换成','
                        String repStr = replaceByDIY(str, EscapeCharacterRegEx);
                        //按,分割成字符串数组
                        String[] newArr = repStr.split(",");
                        ArrayList<String> colorList = new ArrayList<String>();
                        ArrayList<String> noColorList = new ArrayList<String>();
                        for (String newStr : newArr) {
                            if (newStr.contains("色")) {
                                colorList.add(newStr);//获取包含色的字符串,添加到集合中
                            } else {
                                noColorList.add(newStr);//获取没有色的字符串,添加到集合中
                            }
                        }
                        if (colorList.size() == 1) {
                            //这个字符串中包含1个色字,对色先进行替换
                            colorHS.addAll(replaceSeStr(fieldRule, colorList.get(0)));
                            //循环对没有色字再进行替换
                            if (noColorList.size() > 0) {
                                for (String newStr : noColorList) {
                                    if (newStr.length() > 0) {
                                        colorHS.addAll(replaceNoSeStr(fieldRule, newStr));
                                    }
                                }
                            }
                        } else if (colorList.size() < 1) {
                            //没有包含色,则重新从color规则中重新匹配
                            for (String newStr : newArr) {
                                if (newStr.length() > 0) {
                                    colorHS.addAll(replaceNoSeStr(fieldRule, newStr));
                                }
                            }
                        } else {
                            //循环对含有色字字符串进行先替换
                            for (String souColor : colorList) {
                                colorHS.addAll(replaceSeStr(fieldRule, souColor));
                            }
                            //循环对没有色字再进行替换
                            if (noColorList.size() > 0) {
                                for (String newStr : noColorList) {
                                    if (newStr.length() > 0) {
                                        colorHS.addAll(replaceNoSeStr(fieldRule, newStr));
                                    }
                                }
                            }
                        }
                    }
                }
                if (colorHS.size() > 0) {
//                    sb.append("[");
//                    for (String colStr : colorHS) {
//                        sb.append("\""+colStr + "\",");
//                    }
//                    fieldAlias = sb.substring(0, sb.length() - 1)+"]";
                    //返回一个数组
                    return colorHS.toArray();
                } else {
                    return null;
                }
            }
        } else {
            //如果不是color字段，则进入matchAliasName()这个方法执行
            return matchAliasName(fieldKey, rawFieldValue, etlFieldsRules);
        }
        LOG.debug("FieldKey: " + fieldKey + "RawFieldValue: " + rawFieldValue + "FieldAlias: " + fieldAlias);
        return fieldAlias;
    }

    /**
     * 对没有色的字符串进行替换
     */
    private static HashSet<String> replaceNoSeStr(ETLFieldsRules.ETLFieldRule fieldRule, String newStr) {
        //存放颜色集合,获取包含色的字符串,添加到集合中
        HashSet<String> hs = new HashSet<String>();
        //将数字和字母替换成','
        newStr = replaceByDIY(newStr, NumberAndLetterRegEx);
        String[] lastArr = newStr.split(",");
        for (String lastColor : lastArr) {
            if (lastColor.length() > 0) {
                //匹配得到的字符串
                lastColor = fieldRule.getValueFromAtlValueNew(lastColor, false);
                if (lastColor != null) {
                    //只存储不同颜色的字符串.
                    hs.add(lastColor);
                }
            }
        }
        return hs;
    }

    /**
     * 对含有色的字符串进行替换
     */
    private static HashSet<String> replaceSeStr(ETLFieldsRules.ETLFieldRule fieldRule, String souColor) {
        //存放颜色集合
        HashSet<String> hs = new HashSet<String>();
        //去掉色后面的字符串
        String color = souColor.substring(0, souColor.lastIndexOf("色") + 1);
        //将数字和字母替换成','
        color = replaceByDIY(color, NumberAndLetterRegEx);
        String[] lastArr = color.split(",");
        //得到最后包含色字的字符串
        String lastColor = lastArr[lastArr.length - 1];
        //匹配得到的字符串
        lastColor = fieldRule.getValueFromAtlValueNew(lastColor, true);
        if (lastColor != null) {
            //只存储不同颜色的字符串.
            hs.add(lastColor);
        }
        return hs;
    }

    /**
     * 根据根据正则表达式,将自定义字符串替换成','
     */
    private static String replaceByDIY(String sou, String par) {
        Pattern p = Pattern.compile(par);
        Matcher m = p.matcher(sou);
        return m.replaceAll(",");
    }

    /**
     * 获取数值
     * add by yfh
     *
     * @param value
     * @return
     */
    public static String getNumValue(String value) {
        Pattern pt = Pattern.compile("[^0-9]");
        Matcher matcher = pt.matcher(value);
        String numValue = matcher.replaceAll("");
        return numValue;
    }

    /**
     * 过滤SKU数据
     *
     * @param jsonObject
     * @param nameFilters
     * @param namePriceFilters
     * @return
     */
    public static boolean filterSKUData(JSONObject jsonObject, List<String> nameFilters, List<DataFilters.NamePriceFilter> namePriceFilters) {
        String name = jsonObject.getString("name");

        if (name == null || name.trim().length() == 0) {
            return false;
        }

        //过滤name
        for (String filter : nameFilters) {

            //COS(需排除掉品牌“cos”)
            if (filter.equals("COS")) {
                String brand = jsonObject.getString("brand");
                if (name.contains(filter) && !brand.toLowerCase().contains("cos"))
                    return false;
            } else if (filter.contains("+")) {
                String[] filters = filter.split("\\+");
                boolean flag = true;
                for (String f : filters) {
                    flag = flag && name.contains(f);
                }
                if (flag)
                    return false;
            } else if (name.contains(filter))
                return false;
        }

        //过滤name+price
        double price = jsonObject.getDouble("discountprice");
        for (DataFilters.NamePriceFilter npf : namePriceFilters) {
            if (name.contains(npf.name) && price <= npf.price)
                return false;
        }

        return true;
    }

    public static void main(String args[]) {
        ETLFieldRulesUtil.init("etlfieldrules.xml", ETLFieldRulesUtil.FileDriver);
        ETLFieldsRules config = ETLFieldRulesUtil.getEtlFieldsRules();
        ETLFieldsRules.ETLFieldRule brandRule = config.getETLFieldConfigByKey("brand");
        HashMap<String, String> atlValues = brandRule.getATLValues();
        HashMap<String, String> enValues = brandRule.getENValues();
        HashMap<String, String> cnValues = brandRule.getCNValues();
        HashMap<String, String> rawValues = brandRule.getRawValues();
        System.out.println("altValues:" + atlValues);
        System.out.println("enValues:" + enValues);
        System.out.println("cnValues:" + cnValues);
        System.out.println("rawValues:" + rawValues);
        System.out.println("----");
        //Object name = FieldAliasProcessor.matchAliasNameNew("color", "A24白色(2粒扣）,A24黑色(2粒扣,A24杏色(2粒扣,A24藏青色(2粒扣,A24天蓝色(2粒扣,A24灰蓝色(2粒扣）,A23白色(一粒扣）,A23黑色(一粒扣）,A23杏色(一粒扣）,A23藏青色(一粒扣）,A23天蓝色(一粒扣）,A23灰蓝色(一粒扣）,A18白色（三粒扣）,A18杏色（三粒扣）,A18藏青色（三粒扣）,A18天蓝色（三粒扣）,A18黑色（三粒扣）,A18灰蓝色（三粒扣）", config);
        //Object name = FieldAliasProcessor.matchAliasNameNew("color", "A-4427蓝+4380红蓝,B-4427黑白+4380红蓝,C-4427白+4380红蓝,D-4427蓝+4883白蓝,E-4427蓝+4120橙,F-4380红蓝+4427牛仔蓝,G-4427蓝+4570蓝,H-4427蓝+4423藏蓝,I-4570蓝+4427,J-4427蓝+4348蓝灰,K-4427白+4570蓝,L-4427蓝+4634白,M-4427蓝+4570白深蓝,N-4427白+4423藏蓝,O-4427黑白+4423藏蓝,P-4883白蓝+4427黑白,Q-4883白蓝+4570白深蓝,R-4883+4570蓝,S-4427蓝+4348红,T-4380红蓝+4883白蓝,U-4570蓝+4423蓝,V-4883+4427白,W-4427黑+4570蓝,X-4427白色+4570白浅蓝", config);
        //Object name = FieldAliasProcessor.matchAliasNameNew("color", "2661图片色,2661灰色,2661红色,2661上青,3137红色,3137上青,3137灰色,3137绿色,9999黄色,9999墨绿,9999红色,9999上青", config);
//        Object name = FieldAliasProcessor.matchAliasNameNew("reviewsize", "170/M", config);
//        System.out.println(name);
        //Object name = FieldAliasProcessor.matchAliasNameNew("color", "2661图片色,23透明色", config);
//        if (name.getClass().isArray()) {
//            System.out.println(true);
//            for (Object s : (Object[]) name) {
//                System.out.println(s);
//            }
//        } else {
//            System.out.println(false);
//            System.out.println(name);
//        }
//        System.out.println(name);

//        String jsonStr = "{\"id\":{\"item_id\":\"43921990525\",\"sku_id\":\"3211986833654\",\"brand_productid\":\"15HKS0079\"},\"src_url\":\"https://detail.tmall.com/item.htm?id=43921990525&skuId=3211986833654&user_id=866232258&cat_id=50026259&is_b=1&rn=bdccc7acbdee10ed8d0e141e972bcfb1\",\"src_name\":\"天猫\",\"category\":{\"indextype\":\"men\",\"style\":\"pants\",\"description\":\"casualPants\"},\"name\":\"运动裤男长裤冬季加绒加厚秋冬男士休闲裤青少年小脚卫裤男生裤子\",\"tags\":\"\",\"desc\":\"\",\"designer\":{\"name\":\"\",\"desc\":\"\"},\"region\":{\"Area\":\"china\",\"city\":\"\"},\"shelvetime\":\"2016/12/1314:23:11\",\"markettime\":\"2015年秋季\",\"seentime\":\"2016/12/1314:23:11\",\"pd_src_img_url\":\"//img.alicdn.com/bao/uploaded/i3/TB1iOBNLXXXXXbdXXXXXXXXXXXX_!!0-item_pic.jpg_430x430q90.jpg\",\"marketprice\":159,\"discountprice\":89,\"brand\":\"瀚客森\",\"color\":\"白色\",\"material\":\"其他,棉71%聚酯纤维29%\",\"fashionelements\":\"\",\"print\":\"纯色\",\"length\":\"\",\"tech\":\"\",\"style\":\"青春流行,潮\",\"model\":\"修身\",\"sleevemodel\":\"\",\"collarmodel\":\"\",\"waisttype\":\"中腰\",\"shopname\":\"瀚客森旗舰店\",\"size\":\"XXXXL\",\"sales\":\"30454\",\"stock\":\"20\",\"storeup\":\"74522\",\"commentcount\":\"44716\",\"weight\":\"\",\"producedplace\":\"\",\"itemnumber\":\"\",\"otherinfo\":{\"厚薄\":\"薄\",\"基础风格\":\"青春流行\",\"适用场景\":\"其他休闲\",\"面料\":\"其他\",\"细分风格\":\"潮\",\"弹力\":\"微弹\",\"裤长\":\"长裤\",\"工艺处理\":\"免烫处理\",\"销售渠道类型\":\"纯电商(只在线上销售)\",\"适用对象\":\"青少年\",\"款式细节\":\"徽章\",\"材质成分\":\"棉71%聚酯纤维29%\",\"款式\":\"运动裤\",\"裤脚口款式\":\"小脚\"}}";
//        JSONObject json = new JSONObject(jsonStr);
//        double price = json.getDouble("discountprice");
//        if(price < 200)
//            System.out.println(price);
//        Object fieldAlias = FieldAliasProcessor.matchAliasNameNew("brand", " 恒源祥", ETLFieldRulesUtil.getEtlFieldsRules());
//        System.out.println(fieldAlias);
    }
}