# Building a Hive UDF

Hive user-defined functions, or **UDFs**, are custom functions that can be developed in Java, integrated with Hive, and built on top of a Hadoop cluster to allow for efficient and complex computation that would not otherwise be possible with simple SQL. They can be useful and very powerful, and yet online documentation is pretty weak. As a part of my internship during the summer of 2019, I built a Hive UDF used to classify a customers feature/product usage. Online documentation is weak, so this is a start-to-finish tutorial on how to build a simple Hive UDF that reverses a string. 

## Part 1: How Hive UDFs work

#### 1.1 UDFs in Java

There are 2 types of Hive UDF's - simple and generic. Simple UDF's are certainly easier to build, but they are less flexible and certainly less efficient. So we will be building a generic UDF. There are many benefits to building a generic UDF as opposed to a simple UDF:

1. They can handle generic types (trivial).
2. They perform optimally, parsing arguments lazily with no reflective call.
3. They support nested parameters (ex. list of lists)

A generic Hive UDF is written by extending the GenericUDF class:

```

public interface GenericUDF {

    public Object evaluate(DeferredObject[] args) throws HiveException;

    public String getDisplayString(String[] args);

    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException;

}

```

In generic UDFs, all objects are passed around using the object type. **Object inspectors** allow us to read input values from the database and write output values. They can belong to one of the following categories: primitive type, list, map, or struct.

When we first call our UDF, Hive will compute the actual types of the function parameters and call

```public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException;``` 

The method receives one object inspector for each of the arguments of the query, and must return an object inspector for the return type. Hive then uses the returned ObjectInspector to know what the UDF returns and to continue analyzing the query.

After that, rows are passed in to the UDF, which must use the ObjectInspectors it received in `initialize()` to read the deferred objects. UDFs generally stores the ObjectInspectors received and created in `initialize()` in member variables.


#### 1.2 An example

Let's walk though what happens when a call is made to my usage frequency classifier UDF. My UDF has three parameters - a list of dates that a customer has used a product/feature, an evaluation date which is essentially the most recent date that we care about in our classification, and the customers tenure in months. The program will parse these dates and use a Poisson distribution to classify the user as a new, daily, weekly, monthly, or yearly user of that feature. You can call the UDF in a Hive terminal as follows:

`classify_frequency({usage log}, {eval dt}, {tenure})`

So what happens when a call is made to the UDF? First, the `initialize(ObjectInspector[] args)` function is called. `args` should have a length of three, since `classify_frequency` takes in three parameters. So, initialize will first check that `args` has a length of three - it will throw an error if it does not. Next, we check the parameter types to see if they match our specifications. The first parameter should be a list of strings, the second should be a string, and the third should be an integer. Again, the UDF will throw an error if this is not the case.

If the input parameters check out, the UDF will begin processing each row using

```public Object evaluate(DeferredObject[] arg0) throws HiveException```

This is where most of the computation occurs. `evaluate` will first check that the list of strings can be converted into a list of dates, and that the second input parameter can be converted to a date. It will then use a Poisson distribution to determine if a customer is new, daily, weekly, monthly, or yearly, and will return the correct classification as a string.

## Part 2: Let's build a Hive UDF

Let's build a simple Hive UDF that receives as input a string and returns that string reversed. This is quite useless, but should suffice for our purposes.

#### 2.1: Setup

In eclipse, create a new project called `Example`. Within your source folder, create a new package called `StringFunct` and create a Java class within that called `ReverseString.java`.


We will need to add some jar files to our `ReverseString.java` build path. First download each of these jars to your local machine.

1. [commons logging](https://mvnrepository.com/artifact/commons-logging/commons-logging/1.2)   (version 1.2)

2. [haddop core](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-core/1.2.1)   (version 1.2.1)

3. [hive exec](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)   (version 3.1.1)

4. [slf4j api](https://mvnrepository.com/artifact/org.slf4j/slf4j-api)   (version 1.7.26)

5. [slf4j simple](https://mvnrepository.com/artifact/org.slf4j/slf4j-simple)  (version 1.7.26)


Now, left click on the `ReverseString.java` within your project directory, and click `Build Path -> Configure Build Path -> Libraries -> Add External JARs`. Find each of the jar files you just downloaded, add them to your build path and click `OK`.

We can now begin to start building our ReverseString UDF. First, make your `ReverseString.java` class extend the `GenericUDF` interface, and add unimplemented methods. Your class should look something like this:

```
package StringFunct;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ReverseString extends GenericUDF {

    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getDisplayString(String[] arg0) {
        // TODO Auto-generated method stub
        return null;
    }

 
  public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        // TODO Auto-generated method stub
        return null;
   }
}

```

#### 2.2 initialize()

If you recall, when Hive analyzes the query, it computes the actual types of the parameters passed in to the UDF, and calls `initialize()`. This method receives an `ObjectInspector` for each of the arguments in the query (for our UDF, it will receive one string) and returns an `ObjectInspector` with the correct return type (again, a string for our UDF). First, delcare a private variable for our input string object inspector as follows:

`private StringObjectInspector input;`

We can now write our `initialize()` method:

```
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {

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
```

This method is generally trivial - we first check to make sure that our input is of length 1 and is an instance of a `StringObjectInspector` and we throw an error if this is not the case. We then set our private `StringObjectInspector input` to be equal to the first element in our argument array so we can access it within `evaluate()`. Finally, we return a java string object inspector because that is ultimately the return type of our UDF.

 

#### 2.3 evaluate()

After `initialize()` has been called, our table rows are passed to `evaluate()`, which must use the ObjectInspectors it received in `initialize()` to read the deferred objects. These objects should be strings, which we will reverse and return within `evaluate()`. It'll look something like this:


```
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

```

`evaluate()` first does some error checking to make sure our computation will not throw an error. It checks to make sure the object inspector and argument are not `null`, and that the argument is of length 1. If we pass these requirements, we can use the `StringObjectInspector input` that we instantiated in `initialize()` to read the deferred objects, which in our case is `arg[0]` (since we are only passing one argument to our UDF). Working with more complex datatypes (or non primitive datatypes) can be a little more complicated - I suggest you check out my usage frequency classifier repository for an example of how to work with `Lists`.

#### 2.3 Testing our UDF (DON'T SKIP THIS STEP!)

It is imperative that you test the functionality of your UDF before integrating it with Hive. To create a test file, left click on your `ReverseString.java` class and click `New -> JUnit test Case -> give it a name or use default -> Finish`. Here is an test case example demonstrating how Hive UDFs can be instantiated and tested:

```
    @Test
    public void testSimpleString() throws HiveException {
    
        ReverseString r = new ReverseString();
        ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
                                  new ObjectInspector[] { input });
        Text forwards = new Text("hello");
        Object result = r.evaluate(new DeferredObject[] { new DeferredJavaObject(forwards) });

        assertEquals("olleh", resultInspector.getPrimitiveJavaObject(result));

    }

```

Make sure to test for basic functionality, edge cases, and possible some more complicated inputs.

## Part 3: Integrating our UDF with Hive

Now that we have written our UDF, we need to integrate it with Hive.

#### 3.1 Export as JAR

We first need to export our project as a JAR file. Within Eclipse, click `File -> Export -> Java -> JAR file`. Click "Next" and then under "Select the resources to export", click your project, which in this tutorial is `Example`. Make sure the `.classpath` and `.project` boxes are both checked. Choose a directory to save the JAR file and click "Finish".


#### 3.2 Save file in Hadoop and create function in Hive

We can either integrate our function permanently or temporarily within Hive. If you integrate it temporarily, the function will disappear when you close your Hive session.

#### 3.2.1 Integrating permanently (recommendend)

1. Within your querying software (I used MobaXterm), start a new SFTP sesson and upload the JAR file you just created into your local directory.

2. In the terminal, load the JAR file into your Hadoop directory using the following command:

` hadoop fs -put {jar file name}.jar`. For the tutorial the command would be `hadoop fs -put example.jar`. You can check that it's there (and get its path) using the following command: `hfds dfs -ls`. The path to mine is `/user/jgoett179/example.jar`.

3. Next, we want to find out the cluster directory within Hive. To get your cluster path, you can use the command `hdfs getconf -confkey fs.defaultFS`

4. Finally, start a Hive session and create your function using the command:

`CREATE FUNCTION {function name} AS '{package name}.{java file name}' USING JAR '{cluster directory}{path to jar file};`

For the tutorial, the command would be something like this:


`CREATE FUNCTION reverse_string AS 'StringFunct.ReverseString' USING JAR '{cluster directory}{path to jar file}';`


Your function should now be built on top of your cluster, and anybody querying from that cluster will be able to use it.


#### 3.2.1 Integrating temporarily

1. Within querying software (I used MobaXterm), start a new SFTP sesson and upload the JAR file you just created into your local mobaXterm directory ( /home/{ntid} ).

2. Add the JAR file locally using the command `ADD JAR {path to jar file};`

3. Finally, start a Hive session and create your function using the command:

`CREATE TEMPORARY FUNCTION {function name} AS '{package name}.{java file name}';`

#### 3.3 Testing our tutorial UDF

Our tutorial UDF must take in a string, and we can test out it's functionality in Hive. Here is an example query:

`SELECT name, reverse_string(name) AS reversed FROM {database};`

It'll return something like this:

```
name        reversed

jack        kcaj
kate        etak
patrick     kcirtap
sarah       haras
matt        ttam
emily       ylime

```

Useful? No, not really. But still pretty cool!

#### 3.4 Deleting your UDF

If you need to delete your UDF, you can do so using two commands.
 

`hadoop dfs rm -r hdfs://path/to/file` will remove the JAR file from Hadoop. Ex. `hadoop dfs rm -r  {cluster directory}{path to jar file};`

Then, within hive, drop the function as follows: `drop {function name}`. Ex. `drop reverse_string`.

## Conclusion

I hope this tutorial was helpful and that you now have a better understanding of how UDFs work, how they can be built, and how they can be integrated within Hive. If you have any comments or questions, feel free to shoot me an email at jackgoettle23@gmail.com. Good luck and happy coding!



