package StringFunct;

 

import static org.junit.Assert.*;

 

import java.util.ArrayList;

import java.util.List;

 

import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.Text;

import org.junit.Test;

 

public class ReverseStringTest {

      @Test

      public void testSimpleString() throws HiveException {

            ReverseString r = new ReverseString();

            ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

            JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r

                        .initialize(new ObjectInspector[] { input });

 

            Text forwards = new Text("hello");

 

            Object result = r.evaluate(new DeferredObject[] { new DeferredJavaObject(forwards) });

            assertEquals("olleh",

                        resultInspector.getPrimitiveJavaObject(result));

      }

}