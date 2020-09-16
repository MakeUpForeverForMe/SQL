package com.weshare.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ximing.wei
 */
public class DateUtil {

  public static String getDate(String string, String fromFmt, String toFmt) throws ParseException {
    if (EmptyUtil.isEmpty(string)) return null;
    Date date;
    switch (fromFmt) {
      case "ms":
        if (13 != string.length() && 10 != string.length()) return string;
        date = new Date(Long.valueOf(string.length() == 13 ? string : string + "000"));
        break;
      case "s":
        if (10 != string.length()) return string;
        date = new Date(Long.valueOf(string + "000"));
        break;
      default:
        if (string.length() != fromFmt.length()) return string;
        date = new SimpleDateFormat(fromFmt).parse(string);
        break;
    }
    return new SimpleDateFormat(toFmt).format(date);
  }
}
