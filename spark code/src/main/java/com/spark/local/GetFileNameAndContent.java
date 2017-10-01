package com.spark.local;

/* GetFileNameContentForWord2Vec.java */
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Optional;
import com.spark.local.StaticClassLibs.CleanJoins;
import com.spark.local.StaticClassLibs.CleanKey2TxtName;
import com.spark.local.StaticClassLibs.GetDocment;
import com.spark.local.StaticClassLibs.GetTxtName;
import com.spark.local.StaticClassLibs.SwitchKey2Timestamp;
import com.spark.local.StaticClassLibs.rawPath2TxtPath;
import com.spark.local.StaticClassLibs.string2PairRDD;

import scala.Tuple2;

public class GetFileNameAndContent {
	
	private static JavaSparkContext sc;
	private static SparkConf conf;
	private String input_folder, csv_name, period;
	
	public String getCsv_name() {
		return this.csv_name;
	}
	

	public String getInput_folder() {
		return this.input_folder;
	}

	public String getFileNames_folder() {
		return fileNames_folder;
	}

	public String getFileContent_folder() {
		return fileContent_folder;
	}

	private String fileNames_folder;//intermediate result
	private String fileContent_folder;//intermediate result	
	
	public GetFileNameAndContent(String period){
		this.period = period;
		this.fileNames_folder = "fileNames" + period;
		this.fileContent_folder = "fileContents" + period;
		
		this.input_folder = "/Users/xiaofengzhu/Documents/eclipseWorkspace/DocumentRevisionComplete/corpus"+period; 
		this.csv_name = "/Users/xiaofengzhu/Documents/eclipseWorkspace/DocumentRevisionComplete/documentRevision.csv"; 
		
	}
	
	public void checkOutputFolders(){    		
		File matrixFolder = new File(fileContent_folder);

		if (matrixFolder.exists())
			try {
				FileUtils.deleteDirectory(matrixFolder);
			} catch (IOException e) {
				System.err.println("Result folder exists. Fail to delete the result folder");
			}		
	}

	public static void main(String[] args) {

	    conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setMaster("local[*]")
	    .setAppName("GetFileNameAndContent");
	    
	    sc = new JavaSparkContext(conf);	
	    String period;

	    for (int int_period = 1; int_period <= 1; int_period++){
	    	period = int_period + "";	    
			GetFileNameAndContent getFileNameContent = new GetFileNameAndContent(period);
			getFileNameContent.checkOutputFolders();
	
			// using csv
	    	JavaRDD<String> txtNames_rowRdd = sc.textFile(getFileNameContent.getCsv_name());
	    	
	    	JavaPairRDD<String, String> txtNames_timestamps_rdd = txtNames_rowRdd
			.mapToPair(new string2PairRDD());
			
			// folder files
			JavaPairRDD<String, String> raw_fileNameContentsRDD = sc.wholeTextFiles(getFileNameContent.getInput_folder());

			JavaPairRDD<String, String> fileNameContentsRDD = raw_fileNameContentsRDD
					.mapToPair(new rawPath2TxtPath());
			
					
			// join the database csv by txt name
			JavaPairRDD<String, Tuple2<String, Optional<String>>> lojRdd = fileNameContentsRDD
					.leftOuterJoin(txtNames_timestamps_rdd);
			// key unchanged
			JavaPairRDD<String, Tuple2<String,String>> joined_fileNameContentsRDD = lojRdd
					.mapValues(new CleanJoins());
			
			// timestamp , tuple(txt name, content)
			JavaPairRDD<String, Tuple2<String,String>> reversed_fileNameContentsRDD = joined_fileNameContentsRDD
					.mapToPair(new SwitchKey2Timestamp());
			// sort by timestamp descending
			JavaPairRDD<String, Tuple2<String,String>> sorted_reversed_fileNameContentsRDD = 
					reversed_fileNameContentsRDD.sortByKey(false);
			
			System.out.println(sorted_reversed_fileNameContentsRDD.take(10));
			
//			JavaPairRDD<String, String> sorted_fileNameContentsRDD = sorted_reversed_fileNameContentsRDD
//					.mapToPair(new CleanKey2TxtName());
//			
//			// only keep the base names
//			JavaRDD<String> fileNames = sorted_fileNameContentsRDD				
//					.map(new GetTxtName());
//			fileNames.saveAsTextFile(getFileNameContent.getFileNames_folder());
//			
//		    JavaRDD<List<String>> documents = sorted_fileNameContentsRDD	    		
//		    		.map(new GetDocment());
//		    documents.saveAsTextFile(getFileNameContent.getFileContent_folder());
		
	    }
	    // Close the REPL terminal
	    System.exit(0);	    

	}

}
