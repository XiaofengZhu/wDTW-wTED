package com.spark.cluster;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.spark.local.StaticClassLibs.rawPath2TxtPath;

import scala.Tuple2;

public class GetFileNameContentForPV {
	
	private static JavaSparkContext sc;
	private static SparkConf conf;
	private String input_folder, fileContent_folder; //input folder, intermediate result	

	public GetFileNameContentForPV(String input_folder, String fileContent_folder){
		this.fileContent_folder = fileContent_folder;	
		this.input_folder = input_folder; 
		
	}
	
	public String getInput_folder() {
		return this.input_folder;
	}

	public String getFileContent_folder() {
		return fileContent_folder;
	}

	

	public static void main(String[] args) {

	    conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    //.setMaster("local[*]")
	    .setAppName("GetFileNameContentForParagraphVector")
		.set("spark.kryoserializer.buffer.max", "1g")
		.set("spark.yarn.executor.memoryOverhead", "4096")		
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
	    .set("spark.driver.allowMultipleContexts", "true"); 
	    
	    sc = new JavaSparkContext(conf);	    
    	GetFileNameContentForPV getFileNameContent = new GetFileNameContentForPV(args[0], args[1]);
		System.err.println(args[0]);
		// folder files
		JavaPairRDD<String, String> raw_fileNameContentsRDD = sc.wholeTextFiles(getFileNameContent.getInput_folder());

		JavaPairRDD<String, String> fileNameContentsRDD = raw_fileNameContentsRDD
				.mapToPair(new rawPath2TxtPath());
		JavaRDD<String>  sorted_fileNameContentsRDD = fileNameContentsRDD
				.map(new Function<Tuple2<String, String>, String>() {

					@Override
					public String call(
							Tuple2<String, String> x) throws Exception {
						// base name
						String index = x._1();
						// content
						String value = x._2();
						
						StringBuilder vectorStringBuilder = new StringBuilder();
						String spaces = "[\\s\\r]+";
						Pattern pattern = Pattern.compile(spaces);
						String [] paras = value.split("\n");
						for (int i = 0; i < paras.length; i++){
							String para = paras[i];
							Matcher matcher = pattern.matcher(para.trim());
							para = matcher.replaceAll(" ");
							if (para.length() >= 1) {
								vectorStringBuilder.append(para + "****");
							}						
						}
						return index + "****"+ vectorStringBuilder.toString();
					}
					
				});
		// end of saving the base names
		sorted_fileNameContentsRDD.saveAsTextFile(getFileNameContent.getFileContent_folder());

	    // Close the REPL terminal
	    System.exit(0);	    

	}

}
