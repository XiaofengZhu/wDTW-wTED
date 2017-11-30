package com.spark.local;
/* ReadDataFromMySQL */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class ReadDataFromMySQL implements Serializable {

    /**
	 * Spark reads fileName MySql table
	 */
	private static final long serialVersionUID = 1L;
	// initialization demo
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static String MYSQL_USERNAME = "***";
    private static String MYSQL_PWD = "***";
    private static String MYSQL_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/test?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
    // Query example
    private static String MYSQL_QUERY = "(select fileName, datePublished from documentRevision"+
    		"order by datePublished desc) as txtName_timestamp_table";
    private String field1, field2, tablename;
    private static SQLContext sqlContext;
    
    // constructor

	public ReadDataFromMySQL(SetParameters setParameters, JavaSparkContext sc){
		sqlContext = new SQLContext(sc);
		MYSQL_USERNAME = setParameters.getUsername();
		MYSQL_PWD = setParameters.getPassword();
		MYSQL_CONNECTION_URL =
	            "jdbc:mysql://localhost:3306/" + setParameters.getDatabaseName() + 
	            "?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
		this.field1 = setParameters.getTxtFileNameFieldFromFileTable(); 
		this.field2 =  setParameters.getTimestampFieldFromFileTable();
		this.tablename = setParameters.getTableName();	
	}
	
	public ReadDataFromMySQL(String database, String username, String password, 
			JavaSparkContext sc){
		sqlContext = new SQLContext(sc);
		MYSQL_USERNAME = username;
		MYSQL_PWD = password;
		MYSQL_CONNECTION_URL =
	            "jdbc:mysql://localhost:3306/" + database + 
	            "?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
	}


	public List<Row> read(){
		MYSQL_QUERY = "(select "+this.field1+", "+this.field2+" from "+ this.tablename +
				" order by "+field2+" desc) as txtName_timestamp_table";		
        //Data source options
        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("dbtable",MYSQL_QUERY);        
        options.put("partitionColumn", "2");
        options.put("lowerBound", "10001");
        options.put("upperBound", "499999");
        options.put("numPartitions", "10");

        //Load MySQL query result as DataFrame
        @SuppressWarnings("deprecation")
		DataFrame jdbcDF = sqlContext.load("jdbc", options);
        List<Row> txtNamesRows = jdbcDF.collectAsList();   
        return txtNamesRows;
	}
	
	public List<Row> read(String field1, String field2, String tablename){
		MYSQL_QUERY = "(select "+field1+", "+field2+" from "+ tablename +
				" order by "+field2+" desc) as txtName_timestamp_table";		
        //Data source options
        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("dbtable",MYSQL_QUERY);        
        options.put("partitionColumn", "2");
        options.put("lowerBound", "10001");
        options.put("upperBound", "499999");
        options.put("numPartitions", "10");

        //Load MySQL query result as DataFrame
        @SuppressWarnings("deprecation")
		DataFrame jdbcDF = sqlContext.load("jdbc", options);
        List<Row> txtNamesRows = jdbcDF.collectAsList();   
        return txtNamesRows;
	}
	
	public List<String> read(String field1, boolean section,
			String field3, String tablename){
		List<String> sectionStrings= new ArrayList<String>();
		MYSQL_QUERY = "(select "+field1+" from "+ tablename +
				" where subject = '" + field3 + "' ) as section_table";		
        //Data source options
        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("dbtable",MYSQL_QUERY);        
        options.put("partitionColumn", "2");
        options.put("lowerBound", "10001");
        options.put("upperBound", "499999");
        options.put("numPartitions", "10");

        //Load MySQL query result as DataFrame
        @SuppressWarnings("deprecation")
		DataFrame jdbcDF = sqlContext.load("jdbc", options);
        List<Row> txtNamesRows = jdbcDF.collectAsList();  
        
        for (Row r: txtNamesRows){
        	sectionStrings.add(r.toString());
        }
        return sectionStrings;
	}
	// main function
    public static void main(String[] args) {
    	// an example
    	JavaSparkContext sc =new JavaSparkContext(new SparkConf()
    	.setAppName("SparkJdbc").setMaster("local[*]"));    	
    	ReadDataFromMySQL readDataFromMySQL = new ReadDataFromMySQL("test", "root", "mysql", sc);
    	
    	List<String> rs_section_strings = readDataFromMySQL
    			.read("object", true, "ppt-Ch4_copy-18-1-2016-05-08-00-342.txt", "pptSection"); 
    	for (String o: rs_section_strings){
    		System.out.println(o);
    	}
    	sc.close();
//    	List<Row> txtNamesRows = readDataFromMySQL
//    			.read("txtName", "datePublished", "documentRevision4");
//    	// convert List<Row> to JavaRDD<Row>
//    	JavaRDD<Row> txtNames_rowRdd = sc.parallelize(txtNamesRows); 
//    	// convert JavaRDD<Row> to JavaPairRDD<String, String>
//		JavaPairRDD<String, String> txtNames_timestamps_rdd = txtNames_rowRdd
//		.mapToPair(new PairFunction<Row, String, String>(){
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(Row x)
//					throws Exception {
//				String [] row = x.toString().split(",");
//				String txtName = row[0];
//				String timestamp = row[1];	
//				return new Tuple2<String, String>(txtName, timestamp);
//		}});
//		// print out the first five records
//		System.out.println(txtNames_timestamps_rdd.take(5));  
    	
    }
}
