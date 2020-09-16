package com.weshare.utils;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

/**
 * @author ximing.wei
 */
public class EmptyUtil {
  public static boolean isEmpty(String string) {
    return string == null ||
        string.trim().isEmpty() ||
        string.isEmpty() ||
        "null".equals(string.toLowerCase()) ||
        "na".equals(string.toLowerCase());
  }

  public static boolean isEmpty(DeferredObject string) throws HiveException {
    return string == null ||
        string.get() == null ||
        string.get().toString().isEmpty() ||
        string.get().toString().trim().isEmpty() ||
        "null".equals(string.get().toString().toLowerCase()) ||
        "na".equals(string.get().toString().toLowerCase());
  }
}