package com.weshare.udf;

import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * @author ximing.wei
 */
@Description(
    name = "is_empty",
    value = "_FUNC_(PRIMITIVE primitive [, String string || Integer int])",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_(null) as t;\n" +
        "    null\n" +
        "  SELECT _FUNC_('null', 0) as t;\n" +
        "    0\n" +
        "  SELECT _FUNC_('aa') as t;\n" +
        "    aa\n"
)
public class IsEmpty extends UDF {
  public String evaluate(String string) {
    if (EmptyUtil.isEmpty(string)) return null;
    return string;
  }

  public String evaluate(String string, String defaultValue) {
    if (EmptyUtil.isEmpty(string)) return defaultValue;
    return string;
  }

  public String evaluate(String string, int defaultValue) {
    if (EmptyUtil.isEmpty(string)) return String.valueOf(defaultValue);
    return string;
  }
}
