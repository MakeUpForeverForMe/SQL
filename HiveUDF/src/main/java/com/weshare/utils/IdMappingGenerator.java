package com.weshare.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * id mapping
 * 多个系统 按照统一规则生成ID
 * ETL时任意业务ID(手机号、imei、银行卡卡号、护照号、房产编号、车牌号、)如果能mapping到省份证号那么需要把该id对应的标签迁移到
 * 公司统一id的事实表里面去
 *
 * @author wushujia、ximing.wei
 */
public class IdMappingGenerator {
  private static final String SALT = "wsdSWedE34dcAKJHnLYGBSfgKfase2OU3dss";

  /**
   * 单业务id生成，返回hash值
   *
   * @param key  业务ID
   * @param type 1:明文  2:SHA-256密文
   * @return 返回密文
   */
  public static String idGenerate(String key, int type) {
    if (EmptyUtil.isEmpty(key)) return null;
    switch (type) {
      case 1:
        key = EncoderHandler.encodeBySHA256(key);
        break;
      case 2:
        break;
      default:
        return null;
    }
    return EncoderHandler.encodeBySHA256(key + SALT);
  }

  /**
   * 生成内部唯一ID
   *
   * @param encrypt   传入需要 Hash 的字段
   * @param fieldType idNumber    // 身份证号
   *                  passport    // 护照编号
   *                  address     // 详细地址
   *                  userName    // 用户姓名
   *                  phone       // 手机号码
   *                  bankCard    // 银行卡号码或信用卡号码
   *                  imsi        // imsi、meid 手机唯一id
   *                  imei        // imei、idfa、idfv
   *                  plateNumber // 车牌号码
   *                  houseNum    // 房产编号
   * @param type      1:明文  2:SHA-256密文
   * @return 返回密文
   */
  public static String idGenerate(String encrypt, String fieldType, int type) {
    if (EmptyUtil.isEmpty(encrypt)) return null;
    String sign;
    switch (fieldType) {
      case "idNumber":    // 身份证号
        sign = "a_";
        break;
      case "passport":    // 护照编号
        sign = "b_";
        break;
      case "address":     // 详细地址
        sign = "c_";
        break;
      case "userName":    // 用户姓名
        sign = "d_";
        break;
      case "phone":       // 手机号码
        sign = "e_";
        break;
      case "bankCard":    // 银行卡号码或信用卡号码
        sign = "f_";
        break;
      case "imsi":        // imsi、meid 手机唯一id
        sign = "g_";
        break;
      case "imei":        // imei、idfa、idfv
        sign = "h_";
        break;
      case "plateNumber": // 车牌号码
        sign = "i_";
        break;
      case "houseNum":    // 房产编号
        sign = "j_";
        break;
      default:            // 其他默认
        sign = "@_";
        break;
    }
    return sign + idGenerate(encrypt, type);
  }

//    /**
//     * 生成内部唯一ID
//     *
//     * @param key
//     * @param idPre 身份证:a_、护照:b_、银行卡或信用卡:c_、手机号:d_、imsi/meid:e_、imei/idfa/idfv:f_、车牌号:g_、房产编号:h_
//     * @param type  1:明文  2:SHA-256密文
//     * @return
//     */
//    public static String idGenerate(String key, String idPre, int type) {
//        return idPre + idGenerate(key, type);
//    }

  /**
   * 优先级从上到下
   * 前期先返回简单的hash编码字符串
   *
   * @param idNumber    身份证
   * @param passport    护照
   * @param bankCard    银行卡或信用卡
   * @param phone       手机号
   * @param imsi        imsi\meid 手机号唯一id
   * @param imei        imei\idfa\idfv
   * @param plateNumber 车牌号
   * @param houseNum    房产编号
   * @param type        1:明文  2:SHA-256密文
   * @return 返回密文
   */
  public static String idGenerate(String idNumber, String passport, String bankCard, String phone, String imsi, String imei, String plateNumber, String houseNum, int type) {
    if (!StringUtils.isEmpty(idNumber) && !"null".equalsIgnoreCase(idNumber)) {
      return "a_" + idGenerate(idNumber, type);  // 人的key 下面的以后具备idmapping库都需要进行转换成身份证(或者最优)key
    } else if (!StringUtils.isEmpty(passport) && !"null".equalsIgnoreCase(passport)) {
      return "b_" + idGenerate(passport, type);
    } else if (!StringUtils.isEmpty(bankCard) && !"null".equalsIgnoreCase(bankCard)) {
      return "c_" + idGenerate(bankCard, type);
    } else if (!StringUtils.isEmpty(phone) && !"null".equalsIgnoreCase(phone)) {
      return "d_" + idGenerate(phone, type);
    } else if (!StringUtils.isEmpty(imsi) && !"null".equalsIgnoreCase(imsi)) {
      return "e_" + idGenerate(imsi, type);
    } else if (!StringUtils.isEmpty(imei) && !"null".equalsIgnoreCase(imei)) {
      return "f_" + idGenerate(imei, type);
    } else if (!StringUtils.isEmpty(plateNumber) && !"null".equalsIgnoreCase(plateNumber)) {
      return "g_" + idGenerate(plateNumber, type);
    } else if (!StringUtils.isEmpty(houseNum) && !"null".equalsIgnoreCase(houseNum)) {
      return "h_" + idGenerate(houseNum, type);
    } else {
      return null;
    }
  }

  /**
   * 会查找库进行最优id转换
   * ,等具备idmapping库后，对于人的这个维度统一返回基于身份证的key
   *
   * @param idNumber    身份证
   * @param passport    护照
   * @param bankCard    银行卡或信用卡
   * @param phone       手机号
   * @param imsi        imsi\meid 手机号唯一id
   * @param imei        imei\idfa\idfv
   * @param plateNumber 车牌号
   * @param houseNum    房产编号
   * @param type        1:明文  2:SHA-256密文
   * @return 返回密文
   */
  public static String idGenerationBest(String idNumber, String passport, String bankCard, String phone, String imsi, String imei, String plateNumber, String houseNum, int type) {
    // TODO 通过id库进行最优ID转换
    return idGenerate(idNumber, passport, bankCard, phone, imsi, imei, plateNumber, houseNum, type);
  }
}
