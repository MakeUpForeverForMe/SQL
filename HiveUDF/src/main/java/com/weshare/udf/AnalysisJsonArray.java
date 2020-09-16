package com.weshare.udf;

import com.alibaba.fastjson.JSON;
import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;
import java.util.Map;

/**
 * @author ximing.wei
 */
@Description(
    name = "json_array_to_array",
    value = "_FUNC_(String jsonArray)",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_('[{\"aa\":\"bb\"},{\"aa\":\"cc\"}]') as array;\n" +
        "    [{\"aa\":\"bb\"},{\"aa\":\"cc\"}]"
)
public class AnalysisJsonArray extends UDF {
  public List<Map<String, String>> evaluate(String jsonArray) {
    if (EmptyUtil.isEmpty(jsonArray)) return null;
    return JSON.parseObject(jsonArray, List.class);
  }
}
