package StringFunct;

 

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

 

public class ReverseString extends GenericUDF {

     

      private StringObjectInspector input;

     

      public Object evaluate(DeferredObject[] arg0) throws HiveException {

            if (input == null || arg0.length != 1 || arg0[0].get() == null) {

                  return null;

            }

            String forwards = input.getPrimitiveJavaObject(arg0[0].get()).toString();

            return reverse(forwards);

      }

     

      private static String reverse(String in) {

            int l = in.length();

            StringBuilder sb = new StringBuilder();

           

            for (int i = 0; i < l; i++) {

                  sb.append(in.charAt(l - i - 1));

            }

            return sb.toString();

      }

 

      public String getDisplayString(String[] arg0) {

            return "this function takes as input a string and returns that string reversed.";

      }

 

      public ObjectInspector initialize(ObjectInspector[] arg0)

                  throws UDFArgumentException {

            // check to make sure the input has 1 argument

            if (arg0.length != 1) {

                  throw new UDFArgumentException("input must have length 1");

            }

            // create an ObjectInspector for the input

            ObjectInspector input = arg0[0];

           

            // check to make sure the input is a string

            if (!(input instanceof StringObjectInspector)) {

                  throw new UDFArgumentException("input must be a string");

            }

           

            this.input = (StringObjectInspector) input;

           

            System.out.println("Success. Input formatted correctly");

           

            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

      }

 

}
