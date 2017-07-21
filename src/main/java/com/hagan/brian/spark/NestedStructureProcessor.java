package com.hagan.brian.spark;

import java.util.ArrayList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.types.*;

/*
 * Creating columns from the fields of a nested json object using SparkSQL
 * 
 * SparckSQl Concepts Covered:
 * 
 * SQlContext
 * DataFrames
 * Schemas
 * StructTypes
 * Field sets
 * SQL query
 * DataFrame Join
 * 
 * Have you ever wanted/needed to or thought about turning this:
 * 
 *  {"visitor":"Vincent", "profile":"user", "eventType":"communication", "address":{"city":"Columbus","state":"Ohio", "zip":"21000" }}
 *  
 *  Into this using Spark:
 *  
 *  +-------------+-------+-------+--------+-----+-----+
|    eventType|profile|visitor|    city|state|  zip|
+-------------+-------+-------+--------+-----+-----+
|communication|   user|    Yin|Columbus| Ohio|21000|
|communication|   user|  Brian| Orlando|   FL|32765|
|communication|   user|  Henry| Orlando|   FL|32765|
|communication|   user|Derrick|St Cloud|   FL|32822|
+-------------+-------+-------+--------+-----+-----+
 *  
 * 
 * This program will process a json file, in which one field is a nested structure. For example, notice the "data" field.
 * 
 * {"visitor":"Yin", "profile":"user", "eventType":"communication", "address":{"city":"Columbus","state":"Ohio", "zip":"21000" }}
 * 
 * The program first loads the json into a DataFrame (df1):
 * 
 * +--------------------+-------------+-------+-------+
|                data|    eventType|profile|visitor|
+--------------------+-------------+-------+-------+
|[Columbus,Ohio,21...|communication|   user|    Yin|
|  [Orlando,FL,32765]|communication|   user|  Brian|
|  [Orlando,FL,32765]|communication|   user|  Henry|
| [St Cloud,FL,32822]|communication|   user|Derrick|
+--------------------+-------------+-------+-------+

root
 |-- data: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- state: string (nullable = true)
 |    |-- zip: string (nullable = true)
 |-- eventType: string (nullable = true)
 |-- profile: string (nullable = true)
 |-- visitor: string (nullable = true)
 
 * 
 * Now, I could just select df1.data.city, but my goal was to pull the internal fields out into their own columns.
 * 
 * The program then iterates over the df1 fields and creates a StructField[] from any field of type "struct." 
 * Next the program simply selects the struct field from df1 and creates a second DataFrame (df2):
 * 
 * +--------------------+
|                data|
+--------------------+
|[Columbus,Ohio,21...|
|  [Orlando,FL,32765]|
|  [Orlando,FL,32765]|
| [St Cloud,FL,32822]|
+--------------------+
*
*	
	root
 |-- data: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- state: string (nullable = true)
 |    |-- zip: string (nullable = true)
 * 
 * 
 * The program then creates a new DataFrame (df3) by extracting the nested fields from the df2.
 * 
 * +--------+-----+-----+
|    city|state|  zip|
+--------+-----+-----+
|St Cloud|   FL|32822|
|Columbus| Ohio|21000|
| Orlando|   FL|32765|
+--------+-----+-----+

root
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip: string (nullable = true)
 * 
 * 
 * The program will then join the original DataFrame (df1) to this DataFrame where df1.address.zip == df3.zip. For example,
 * 
+-------------+-------+-------+--------+-----+-----+
|    eventType|profile|visitor|    city|state|  zip|
+-------------+-------+-------+--------+-----+-----+
|communication|   user|Vincent|Columbus| Ohio|21000|
|communication|   user|  Brian| Orlando|   FL|32765|
|communication|   user|  Henry| Orlando|   FL|32765|
|communication|   user|Derrick|St Cloud|   FL|32822|
+-------------+-------+-------+--------+-----+-----+
 * 
 * root
 |-- eventType: string (nullable = true)
 |-- profile: string (nullable = true)
 |-- visitor: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip: string (nullable = true)
 * 
 */

 
public class NestedStructureProcessor {
	
	
    public static void main(String[] args) {
    	SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	SQLContext sqlContext = new SQLContext(sc);    
    	DataFrame df = sqlContext.read().json("file://" + args[0]);    	
    	df.printSchema();
    	
    	    	
    	
    	//Add an index to the RDD
    	JavaRDD<Row> jrdd = addIndex(sc, sqlContext, df);
    	
    	//Create a schema for to use when creating a dataframe
    	StructType st = updateSchemaWithIndex(sc, sqlContext,df);
    	
    	//Create DataFrame with New Schema that includes the index
		DataFrame newDF = sqlContext.createDataFrame(jrdd, st);    
		
		//When you create a dataframe from an RDD that has been zipped with index,
		//the all of the fields are part of a struct, including the index
    	DataFrame nestedColumns1 = getNestedColumnNames(sqlContext, newDF); 
    	DataFrame nestedColumns2 = getNestedColumnNames(sqlContext, nestedColumns1); 
    	DataFrame joined = nestedColumns1.join(nestedColumns2, "id");
    	DataFrame addressDropped = joined.drop("address");
    	addressDropped.show();

    	System.exit(0);
    }
 
    public static JavaRDD<Row> addIndex(JavaSparkContext sc, SQLContext sqlc, DataFrame df) {
    	return df.javaRDD().zipWithIndex().map(indexedRow -> RowFactory.create(indexedRow._1(), indexedRow._2()));    	
    }
    
    public static StructType updateSchemaWithIndex(JavaSparkContext sc, SQLContext sqlc, DataFrame df) {
    	StructType schema = df.schema();
    	StructType st = DataTypes.createStructType(schema.fields());
    	StructField jsonRow = new StructField("row", st, true, Metadata.empty());
    	StructField id = new StructField("id", DataTypes.LongType, true, Metadata.empty());
    	StructType newSchema = new StructType().add(jsonRow).add(id);
    	
    	return newSchema;
    }
       
    public static DataFrame getNestedColumnNames(SQLContext sqlc, DataFrame df1) {
    	StructField[] df1Fields = df1.schema().fields();  
        ArrayList<String> columnNames = new ArrayList<String>();
        DataFrame df3 = null;

    	for (int y = 0; y < df1Fields.length; y++) {

    		if(df1Fields[y].dataType().typeName().equalsIgnoreCase("struct")) {
    			DataFrame df2 = df1.select(df1Fields[y].name());
    			StructType df2Schema = df2.schema();
    			StructType df2Fields = (StructType) df2Schema.fields()[df2Schema.fieldIndex(df1Fields[y].name())].dataType();

    			for(StructField InnerField : df2Fields.fields()) {
    				columnNames.add(InnerField.name());
    			}
    			
            	df1.registerTempTable("visitor");
            	String q = " ";
            	for(int i = 0; i <= columnNames.size() - 1; i++) {
            		if(i == columnNames.size() - 1)
            			q += df1Fields[y].name() + "." + columnNames.get(i) ;
            		else
            			q += df1Fields[y].name() + "." + columnNames.get(i)  + ", ";
            	}
            	
            	String query = "select id, " + q + " from visitor";
                df3 = sqlc.sql(query);  
                StructField[] df3Fields = df3.schema().fields(); 
                //df3.show();

    		}   
    	}    	   	
		return df3;
    }
    
    public static boolean checkForStruct(DataFrame df) {
    	StructField[] dfFields = df.schema().fields();  

    	for (int y = 0; y < dfFields.length;y++) {
    		if(dfFields[y].dataType().typeName().equalsIgnoreCase("struct")) {
    			return true;
    		} 
    	}
		return false;
    }
    
    public static ArrayList<String> getStructFieldNames(DataFrame df) {
    	StructField[] dfFields = df.schema().fields();  
    	ArrayList<String> structFieldsNames = new ArrayList<String>();
    	
    	for (int y = 0; y < dfFields.length;y++) {
    		if(dfFields[y].dataType().typeName().equalsIgnoreCase("struct")) {
    			structFieldsNames.add(dfFields[y].name());
    		} 
    	}
    	return structFieldsNames;
    }
    
    public static DataFrame getNestedColumnNames2(SQLContext sqlc, DataFrame df1) {
    	StructField[] df1Fields = df1.schema().fields();  
        ArrayList<String> columnNames = new ArrayList<String>();
        
        DataFrame df3 = null;
        //while(true) {
	    	if (checkForStruct(df1)) {
	    		ArrayList<String> structFieldNames = getStructFieldNames(df1);
	
	    		for( int y = 0; y < structFieldNames.size(); y++) {
	     			
	    			DataFrame df2 = df1.select(structFieldNames.get(y));
	    			StructType df2Schema = df2.schema();
	    			StructType df2Fields = (StructType) df2Schema.fields()[df2Schema.fieldIndex(structFieldNames.get(y))].dataType();
	
	    			for(StructField InnerField : df2Fields.fields()) {
	    				columnNames.add(InnerField.name());
	    			}
	    			
	            	df1.registerTempTable("visitor");
	            	String q = " ";
	            	for(int i = 0; i <= columnNames.size() - 1; i++) {
	            		if(i == columnNames.size() - 1)
	            			q += df1Fields[y].name() + "." + columnNames.get(i) ;
	            		else
	            			q += df1Fields[y].name() + "." + columnNames.get(i)  + ", ";
	            	}
	            	
	            	String query = "select id, " + q + " from visitor";
	                df3 = sqlc.sql(query);  
	                StructField[] df3Fields = df3.schema().fields(); 
	                df3.show();
	                //df3.select()	
	    		}   
	    	}    
        //}
			return df3;
    }    
    
/*    public static JavaRDD<Row> addIndexWithAnonymousInnerClass() {
    	df.javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                   return RowFactory.create(v1._1().getString(0), v1._2());
            }
    })
		return null;
    	
    }*/
}