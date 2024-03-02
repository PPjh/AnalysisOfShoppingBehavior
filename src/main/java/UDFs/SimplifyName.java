package UDFs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.nio.charset.StandardCharsets;

public class SimplifyName extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数长度判断
        if(arguments.length > 0){
            throw new UDFArgumentLengthException("此函数只作用于<dwd_AOfSb>表的<name>列，不需要输入参数");
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
/*
        if(deferredObjects[0].get() == null){
            return "" ;
        }
        String data = deferredObjects[0].get().toString();
        int index = data.indexOf("?");
        if(index > 0 ){
            data = data.substring(0,index);
        }

        if (data.startsWith("https://")){
            data=data.replaceFirst("https://","http://");
        }

        return new Text(data.getBytes(StandardCharsets.UTF_8));
*/
        String patternStr =  "\\(.*?\\)";
        if(arguments[0].get() == null){
            return "" ;
        }
        String data = arguments[0].get().toString();
        data = data.replaceAll(patternStr,"");
        return new Text(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
