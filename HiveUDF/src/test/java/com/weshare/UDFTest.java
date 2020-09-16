package com.weshare;

import com.weshare.udf.*;
import com.weshare.utils.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.junit.Test;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class UDFTest {
  @Test
  public void encrypt() {
    System.out.println(new AesEncrypt().evaluate("18812345678"));
    System.out.println(new AesEncrypt().evaluate("18812345678", AesPlus.PASSWORD_WESHARE));
  }

  @Test
  public void decrypt() {
    System.out.println(new AesDecrypt().evaluate("AdDesv4O8b9QR5jIZ6hwgw=="));
    System.out.println(new AesDecrypt().evaluate("AdDesv4O8b9QR5jIZ6hwgw==", AesPlus.PASSWORD_WESHARE));
  }

  @Test
  public void dateFormat() throws ParseException {
    System.out.println(new DateFormat().evaluate("20200429203312", "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss"));
    System.out.println(new DateFormat().evaluate(String.valueOf(20200429203312L), "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss"));
    System.out.println(new DateFormat().evaluate(null, "ms", "yyyy-MM-dd HH:mm:ss"));

    System.out.println(new DateFormat().evaluate("1588240046", "s", "yyyy-MM-dd HH:mm:ss.SSS"));
    System.out.println(new DateFormat().evaluate("1588240046812", "ms", "yyyy-MM-dd HH:mm:ss.SSS"));
    System.out.println(new DateFormat().evaluate("20200430181203", "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss.SSS"));
//    System.out.println(new DateFormat().evaluate(20200430181203L, "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss.SSS"));
  }

  @Test
  public void timeTest() throws ParseException {
    // 当前日期
    LocalDate ld = LocalDate.now();
    System.out.println(ld);
    // 当前时间
    LocalTime lt = LocalTime.now();
    System.out.println(lt);
    // 当前日期和时间
    LocalDateTime ldt = LocalDateTime.now();

    System.out.println(ldt);

    // 指定日期和时间
    LocalDate ld2 = LocalDate.of(2016, 11, 30);
    System.out.println(ld2);
    LocalTime lt2 = LocalTime.of(15, 16, 17);
    System.out.println(lt2);
    LocalDateTime ldt2 = LocalDateTime.of(2016, 11, 30, 15, 16, 17);
    System.out.println(ldt2);
    LocalDateTime ldt3 = LocalDateTime.of(ld2, lt2);
    System.out.println(ldt3);

    LocalDateTime parse = LocalDateTime.parse("2016-12-30 12", DateTimeFormatter.ofPattern("yyyy-MM-dd HH"));
    System.out.println(parse);

    String s = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(parse);
    System.out.println(s);
  }


  @Test
  public void sex() throws Exception {
    String[] strings = new String[]{
        "510522198209174418", // 男
        "440510198110230412", // 男
        "36250219840830501X", // 男
        "440521198011182514", // 男
        "510522198209174418", // 女
        "440510198110230412", // 女
        "429006198210081226", // 女
        "440307198308220069", // 女
        "310109196701301627", // 女
        "522528198407040826", // 女
    };
    for (String string : strings) {
      System.out.println(new GetSexOnIdNo().evaluate(string));
    }
  }


  @Test
  public void json() {
    String jsonArray = "[{\"name\":\"向小雄\",\"sequence\":1,\"mobile_phone\":\"13794483952\",\"relationship\":\"C\",\"relational_human_type\":\"RHT01\"},{\"name\":\"王龙\",\"sequence\":2,\"mobile_phone\":\"18566291465\",\"relationship\":\"O\",\"relational_human_type\":\"RHT01\"},{\"name\":\"邵华\",\"sequence\":3,\"mobile_phone\":\"13713857136\",\"relationship\":\"W\",\"relational_human_type\":\"RHT01\"}]";
    System.out.println(new AnalysisJsonArray().evaluate(jsonArray));
  }

  @Test
  public void test() {
    System.out.println("111111 MD5  :" + EncoderHandler.encodeByMD5("111111"));
    System.out.println("111111 SHA1 :" + EncoderHandler.encodeBySHA1("111111"));
    System.out.println("111111 SHA-256 :" + EncoderHandler.encodeBySHA256("111111"));
    System.out.println("111111 SHA-512 :" + EncoderHandler.encodeBySHA512("111111"));
  }

  @Test
  public void test1() {
    String string = "111111";
    String sha256 = EncoderHandler.encodeBySHA256(string);

//    System.out.println(sha256);
    System.out.println(IdMappingGenerator.idGenerate(string, 1)); // c7688a3dffef507eefabec8d819e4c5faee5449c911e5ada453880f2d670d729
    System.out.println(IdMappingGenerator.idGenerate(sha256, 2)); // c7688a3dffef507eefabec8d819e4c5faee5449c911e5ada453880f2d670d729
    System.out.println(IdMappingGenerator.idGenerate(sha256, 3)); // null
  }

  @Test
  public void test2() {
    List<String> list = Arrays.asList("idNumber", "passport", "bankCard", "phone", "imsi", "imei", "plateNumber", "houseNum");

    System.out.println(list.indexOf("idNumber"));

    System.out.println(Arrays.toString("a".getBytes()));
    System.out.println(Arrays.toString("b".getBytes()));
    System.out.println((int) 'a');
    System.out.println((char) 97);
  }

  @Test
  public void sha256HashSalt() {
    String string = "111111";
    String sha256 = EncoderHandler.encodeBySHA256(string);

    System.out.println(sha256);
    System.out.println(IdMappingGenerator.idGenerate(string, 1));
    System.out.println(IdMappingGenerator.idGenerate(sha256, 2));

    System.out.println(IdMappingGenerator.idGenerate(string, "idNumber", 1));
    System.out.println(IdMappingGenerator.idGenerate(sha256, "idNumber", 2));


    System.out.println(new Sha256Salt().evaluate(string, "idNumber", 1));
    System.out.println(new Sha256Salt().evaluate(sha256, "idNumber", 2));
  }

  @Test
  public void test4() {
    String string = "18812345678";
    String sha256 = EncoderHandler.encodeBySHA256(string);

    System.out.println(sha256);
    System.out.println(new Sha256Salt().evaluate(string, "phone", 1));
    System.out.println(new Sha256Salt().evaluate(sha256, "phone", 2));
  }

  @Test
  public void test5() {
    System.out.println(0x01); // 1
  }

  @Test
  public void test6() {
    System.out.println(new Sha256Salt().evaluate("342921198808300119", "idNumber", 1));
    System.out.println(new Sha256Salt().evaluate("18812345678", "phone", 1));
    System.out.println(new Sha256Salt().evaluate("342921198808300119", "t", 1));
  }

  @Test
  public void test7() {
    System.out.println(EmptyUtil.isEmpty("aa"));
    System.out.println(EmptyUtil.isEmpty("\t     \t   \t"));
  }

  @Test
  public void dataIsEmpty1() {
    System.out.println(new IsEmpty().evaluate("aa"));
    System.out.println(new IsEmpty().evaluate(null, "test"));
    System.out.println(new IsEmpty().evaluate(null, 0));
  }

  @Test
  public void dataIsEmpty2() throws HiveException {
    IsEmptyGenericUDF isEmptyGenericUDF = new IsEmptyGenericUDF();

    ObjectInspector[] inputOI = {
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
//        ObjectInspectorFactory.getStandardListObjectInspector(
//            PrimitiveObjectInspectorFactory.javaStringObjectInspector
//        ),
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
//        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
//        PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
    };

    ObjectInspector objectInspector = isEmptyGenericUDF.initialize(inputOI);

    StringObjectInspector stringObjectInspector = null;
    HiveDecimalObjectInspector hiveDecimalObjectInspector = null;
    DoubleObjectInspector doubleObjectInspector = null;
    IntObjectInspector intObjectInspector = null;

    switch (((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory()) {
      case STRING:
        stringObjectInspector = (StringObjectInspector) objectInspector;
        break;
      case DECIMAL:
        hiveDecimalObjectInspector = (HiveDecimalObjectInspector) objectInspector;
        break;
      case DOUBLE:
        doubleObjectInspector = (DoubleObjectInspector) objectInspector;
        break;
      case INT:
        intObjectInspector = (IntObjectInspector) objectInspector;
        break;
    }

    Object result = isEmptyGenericUDF.evaluate(
        new DeferredObject[]{
            new DeferredJavaObject(null),
            new DeferredJavaObject("0")
        }
    );

//    System.out.println(result);

    if (stringObjectInspector != null)
      System.out.println("String |" + stringObjectInspector.getPrimitiveJavaObject(result));
    if (hiveDecimalObjectInspector != null)
      System.out.println("Decimal |" + hiveDecimalObjectInspector.getPrimitiveJavaObject(result));
    if (doubleObjectInspector != null)
      System.out.println("Double |" + doubleObjectInspector.getPrimitiveJavaObject(result));
    if (intObjectInspector != null)
      System.out.println("Integer |" + intObjectInspector.getPrimitiveJavaObject(result));


//    Assert.fail("" + result);
//    Assert.assertTrue(resultInspector.get(result));
//    Assert.assertNull(result);
//    Assert.assertEquals(inputOI,result);
  }

  @Test
  public void idNumberUtil() throws Exception {
    System.out.println(IdNumberUtil.len());
    System.out.println(IdNumberUtil.get18IdNo(""));
    System.out.println(IdNumberUtil.get18IdNo("123456789123456")); // 12345619789123456X
    System.out.println(IdNumberUtil.get18IdNo("110105491231002")); // 11010519491231002X
  }

  @Test
  public void ageOnIdNo() throws Exception {
    System.out.println(new GetAgeOnIdNo().evaluate("11010519491231002X"));
    System.out.println(new GetAgeOnIdNo().evaluate("11010519491231002X", "2020-12-31"));
    System.out.println(new GetAgeOnIdNo().evaluate("522528198407040826"));
    System.out.println(new GetAgeOnIdNo().evaluate("522528198407040826", "2020-07-04"));
  }

  @Test
  public void getAge() {
    System.out.println(AgeUtil.getAge("2000-06-01", "2020-06-24"));
    System.out.println(AgeUtil.getAge("2000-06-01", "2020-05-24"));

    System.out.println(AgeUtil.getAge("2020-06-24", "2000-06-01"));
    System.out.println(AgeUtil.getAge("2020-05-24", "2000-06-01"));
  }

  @Test
  public void CompareUtilTest() {
    System.out.println(CompareUtil.getMaxDate(LocalDate.parse("2020-06-24"), LocalDate.parse("2000-06-01")));
    System.out.println(CompareUtil.getMinDate(LocalDate.parse("2020-06-24"), LocalDate.parse("2000-06-01")));
  }

  @Test
  public void GetDateTest() {
//    System.out.println(new GetDateMax().evaluate("2020-06-24", "2000-06-01"));
//    System.out.println(new GetDateMin().evaluate("2020-06-24", "2000-06-01"));
    System.out.println(new GetDateMin().evaluate("2020-06-24 12:14:00", "2000-06-01 12:14:00"));
    System.out.println(new GetDateMax().evaluate("2020-06-24 12:14:00", "2000-06-01 12:14:00"));
//    System.out.println(new GetDateMax().evaluate("2020-06-24", null));
//    System.out.println(new GetDateMin().evaluate("2000-06-01", null));
  }

  @Test
  public void test11() {
    System.out.println(LocalDateTime.parse("2020-06-24T12:00:10").toString().replace('T', ' '));
  }

  @Test
  public void getAgeOnBirthday() {
    System.out.println(new GetAgeOnBirthday().evaluate("2000-06-01"));
    System.out.println(new GetAgeOnBirthday().evaluate("2000-06-01", "2020-06-24"));
    System.out.println(new GetAgeOnBirthday().evaluate("2000-06-01", "2020-05-24"));
  }

  @Test
  public void testa11() {
    for (int i = 0; i <= 200; i++) {
      System.out.println(i + " : " + (i == 0 ? 0 : (i - 1) / 30 + 1));
    }
  }
}
