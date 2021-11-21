package com.demo;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author kevin
 * @date 2021/11/4
 * @desc hive自定义UDF
 */
public class MyStringLen extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        //判断输入参数的个数
        if(args.length != 1){
            throw new UDFArgumentLengthException("input args length error");
        }
        //判断输入参数类型
        if(!args[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"type error");
        }
        //函数本身返回值为int 需要返回int类型的鉴别器对象
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if(args[0] == null){
            return 0;
        }else{
            return args[0].get().toString().length();
        }

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
