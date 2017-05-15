package cn.datapark.process.sku.preprocess

import cn.datapark.process.sku.config.{ETLFieldsRules, ETLFieldRulesUtil}
import org.json.{JSONArray, JSONObject}

//import net.sf.json.{JSONArray, JSONObject}

/**
  * 处理别名
  *
  * kafka传来的原始数据中的color,print,brand,material添加别名
  * 返回jsonObjectSKU 添加完别名的sku数据
  * Created by cluster on 2017/3/1.
  */
object addFieldAlias {

  val ETLFile = "etlfieldrules.xml"
  ETLFieldRulesUtil.init(ETLFile, ETLFieldRulesUtil.FileDriver)

  def FieldAlias(line: JSONObject): String = {
    val jsonObjectSKU = line
    //读取规则
    val efr: ETLFieldsRules = ETLFieldRulesUtil.getEtlFieldsRules
    //reviewsize,print,material,color,style,brand
    val it = efr.KeyIterator()
    while (it.hasNext) {
      val fName = it.next()
      // 把数据源里的逗号换成 /  然后去比较，因为XML配置项 是按照 , 号分隔的
      val fieldName = fName.replace(",", "/")
      var fieldValue = ""
      try {
        if (jsonObjectSKU.has(fieldName)) {
          fieldValue = jsonObjectSKU.getString(fieldName).trim
        }
      } catch {
        case ex: Exception => {
          val colorArr: AnyRef = jsonObjectSKU.get(fieldName).asInstanceOf[JSONArray]
          fieldValue = colorArr.toString.trim
          //fieldValue = JSONObject.valueToString(colorArr)
          fieldValue = fieldValue.replace("[", "").replace("]", "").replace("\"", "").trim
        }
      }
      if (fieldValue != null) {
        var fieldAlias: AnyRef = FieldAliasProcessor.matchAliasNameNew(fieldName, fieldValue, ETLFieldRulesUtil.getEtlFieldsRules);
        if (fieldAlias != null) {
          //1. 如果 fieldValue 并没有匹配的别名，fieldAlias为空，则不将这个别名字段添加到这条数据中
          //2. *********加此处判断是因为"·"符号在上传到disconf后转为unicode，该unicode无法识别，
          // 所以配置文件中用"."替代，此处再换回"·"
          if (fieldAlias == "Jimmy Choo/吉米.周") {
            fieldAlias = fieldAlias.toString.replace(".", "·")
          }
          jsonObjectSKU.put(fieldName + "_alias", fieldAlias)
        }
      }
      //    此处是判断京东全球购数据，京东全球购数据brand值为空，所以用此程序来填补
      else if (fieldName == "brand" && fieldValue == null && line.getString("src_url").contains("item.jd.hk")) {
        // 对brand值为null的数据，将数据中的name值与配置文件进行匹配，提取出品牌 赋给 brand
        BrandMatch.init("brandmapping.xml")
        //根据某些关键字去截取  eg: 男，女，色，童..防止后面的型号颜色等信息影响匹配
        val name: String = jsonObjectSKU.getString("name")
        val sp: Array[String] = name.split("[男女童色]")
        val namesp: String = sp(0).replaceAll("[·/()（）]", "")
        //将中英文分开
        val regstr: String = BrandMatch.getRegString(namesp)
        val brandname: String = BrandMatch.getBrandName_cec(regstr)
        jsonObjectSKU.put("brand", brandname)
        fieldValue = brandname
        var fieldAlias: AnyRef = FieldAliasProcessor.matchAliasNameNew(fieldName, fieldValue, ETLFieldRulesUtil.getEtlFieldsRules);
        if (fieldAlias != null) {
          //*********加此处判断是因为"·"符号在上传到disconf后转为unicode，该unicode无法识别，
          // 所以配置文件中用"."替代，此处再换回"·"
          if (fieldAlias == "Jimmy Choo/吉米.周") {
            fieldAlias = fieldAlias.toString.replace(".", "·")
          }
          jsonObjectSKU.put(fieldName + "_alias", fieldAlias)
        }
      }
    }
    //返回第一步处理的返回值
    //println(jsonObjectSKU.toString())
    jsonObjectSKU.toString()
  }
}
