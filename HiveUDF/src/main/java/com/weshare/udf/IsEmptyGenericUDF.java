package com.weshare.udf;

import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

/**
 * @author ximing.wei
 */
@Description(
    name = "is_empty2",
    value = "_FUNC_(PRIMITIVE primitive [, String string || Integer int])",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_('aa') as t;\n" +
        "    aa\n" +
        "  SELECT _FUNC_(1,0) as t;\n" +
        "    1\n" +
        "  SELECT _FUNC_('null', 0) as t;\n" +
        "    0"
)
public class IsEmptyGenericUDF extends GenericUDF {

  private StringObjectInspector FIRSTPOISTRING = null;
  private HiveDecimalObjectInspector FIRSTPOIDECIMAL = null;
  private DoubleObjectInspector FIRSTPOIDOUBLE = null;
  private IntObjectInspector FIRSTPOIINT = null;
  private StringObjectInspector SECONDPOISTRING = null;
  private IntObjectInspector SECONDPOIINT = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
    int argsLength = objectInspectors.length;

    if (argsLength != 1 && argsLength != 2)
      throw new UDFArgumentLengthException("传入的参数数量不正确（一个或两个），实际是：" + objectInspectors.length);

    if (argsLength == 2) {
      ObjectInspector second = objectInspectors[1];
      if (!(second instanceof PrimitiveObjectInspector))
        throw new UDFArgumentTypeException(1, "传入的第二个参数类型不适用（" + second.getTypeName() + "），请重新赋值");

      switch (((PrimitiveObjectInspector) second).getPrimitiveCategory()) {
        case STRING:
          this.SECONDPOISTRING = (StringObjectInspector) second;
          break;
        case INT:
          this.SECONDPOIINT = (IntObjectInspector) second;
          break;
        default:
          throw new UDFArgumentTypeException(1, "传入的第二个参数类型不适用（" + second.getTypeName() + "），请重新赋值");
      }
    }

    ObjectInspector first = objectInspectors[0];
    if (!(first instanceof PrimitiveObjectInspector))
      throw new UDFArgumentTypeException(0, "传入的第一个参数类型未设置或传入的参数类型不适用（" + first.getTypeName() + "）");

    switch (((PrimitiveObjectInspector) first).getPrimitiveCategory()) {
      case STRING:
        this.FIRSTPOISTRING = (StringObjectInspector) first;
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
      case DECIMAL:
        this.FIRSTPOIDECIMAL = (HiveDecimalObjectInspector) first;
        return PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
      case DOUBLE:
        this.FIRSTPOIDOUBLE = (DoubleObjectInspector) first;
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
      case INT:
        this.FIRSTPOIINT = (IntObjectInspector) first;
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }
    return null;
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    DeferredObject inputValue = deferredObjects[0];

    if (EmptyUtil.isEmpty(inputValue)) {
      switch (deferredObjects.length) {
        case 1:
          return null;
        case 2:
          DeferredObject defaultValue = deferredObjects[1];
          if (EmptyUtil.isEmpty(defaultValue)) return null;
          else {
            if (this.SECONDPOISTRING != null) return this.SECONDPOISTRING.getPrimitiveJavaObject(defaultValue.get());
            if (this.SECONDPOIINT != null) return this.SECONDPOIINT.getPrimitiveJavaObject(defaultValue.get());
          }
      }
    }

    if (this.FIRSTPOISTRING != null) return this.FIRSTPOISTRING.getPrimitiveJavaObject(inputValue.get());
    if (this.FIRSTPOIDECIMAL != null) return this.FIRSTPOIDECIMAL.getPrimitiveJavaObject(inputValue.get());
    if (this.FIRSTPOIDOUBLE != null) return this.FIRSTPOIDOUBLE.getPrimitiveJavaObject(inputValue.get());
    if (this.FIRSTPOIINT != null) return this.FIRSTPOIINT.getPrimitiveJavaObject(inputValue.get());
    throw new UDFArgumentException("请增加数据类型！");
  }

  @Override
  public String getDisplayString(String[] strings) {
//    assert (strings.length == 1 || strings.length == 2);
    return "is_empty('" + strings[0] + "', '" + strings[1] + "')";
  }
}
