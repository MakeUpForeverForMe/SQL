package com.weshare.udf;

import com.weshare.utils.AesPlus;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
    name = "decrypt",
    value = "_FUNC_(ciphertext [, password]) - Input Ciphertext [Password], Output Cleartext",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_('AdDesv4O8b9QR5jIZ6hwgw=='[,weshare666]);\n" +
        "    '18812345678'"
)
public class AesDecrypt extends UDF {
  public String evaluate(String ciphertext, String password) {
    return new AesPlus().decrypt(ciphertext, password);
  }

  public String evaluate(String ciphertext) {
    return new AesPlus().decrypt(ciphertext, AesPlus.PASSWORD_WESHARE);
  }
}
