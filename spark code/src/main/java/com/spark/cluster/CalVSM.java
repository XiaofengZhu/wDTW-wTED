package com.spark.cluster;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;
import com.spark.local.StaticClassLibs.*;

import scala.Tuple2;

public class CalVSM {
	
	private JavaSparkContext sc;

	private ReadDataFromMySQL readDataFromMySQL;	
	private String csvFilePath, inputPath, fileNameFolder, matrixFolder;
	private boolean isDB = false;
	
	public CalVSM(String inputPath, 
			String fileNameFolder, String matrixFolder, 
			String database, String username, String password, 
			String field1, String field2, String tablename, 
			JavaSparkContext sc ){
		this.inputPath = inputPath;
		this.fileNameFolder = fileNameFolder;
		this.matrixFolder = matrixFolder;
		this.readDataFromMySQL = 
				new ReadDataFromMySQL(database, username, password, 
				field1, field2, tablename, sc);
	    this.sc = sc;
	    this.isDB = (this.csvFilePath == null);
	}
	public CalVSM(String inputPath, String fileNameFolder,
			String matrixFolder, String csvFilePath,
			JavaSparkContext sc ){
		this.csvFilePath = csvFilePath;
		this.inputPath = inputPath;
		this.fileNameFolder = fileNameFolder;
		this.matrixFolder = matrixFolder;
	    this.sc = sc;
	}	
	
	public void cal(){		
		
		JavaPairRDD<String, String> txtNames_timestamps_rdd;
		
		if (this.isDB){
			// default: using database
	    	List<Row> txtNamesRows = this.readDataFromMySQL.read();	
	    	JavaRDD<Row> txtNames_rowRdd = sc.parallelize(txtNamesRows); 
	    	
			txtNames_timestamps_rdd = txtNames_rowRdd
			.mapToPair(new Row2PairRDD());	
			
		}else{
			// using csv
	    	JavaRDD<String> txtNames_rowRdd = sc.textFile(this.csvFilePath);// ,20000 is not required
	    	
			txtNames_timestamps_rdd = txtNames_rowRdd
			.mapToPair(new string2PairRDD());			
		}
  
		
		// folder files
		JavaPairRDD<String, String> raw_fileNameContentsRDD = sc.wholeTextFiles(this.inputPath);//, 20000

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
		
		
		JavaPairRDD<String, String> sorted_fileNameContentsRDD = sorted_reversed_fileNameContentsRDD
				.mapToPair(new CleanKey2TxtName());
		
		// only keep the base names
		JavaRDD<String> fileNames = sorted_fileNameContentsRDD				
				.map(new GetTxtName());
		fileNames.saveAsTextFile(this.fileNameFolder);
		// end of saving the base names
		// System.out.println("fileNames.count(): " +fileNames.count());
		
		// calculate cosine matrix
	    JavaRDD<List<String>> documents = sorted_fileNameContentsRDD	    		
	    		.map(new GetDocment());
	    
	    HashingTF tf = new HashingTF();
	    JavaRDD<Vector> termFreqs = tf.transform(documents);
	    //termFreqs.collect();
	    IDF idf = new IDF(2);
	    // a JavaRDD of local tf-idf vectors
	    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
	    List<IndexedRow> inputList = new ArrayList<IndexedRow>();
	    List<Vector> localTfIdfs = tfIdfs.collect();

	    for (int i = 0; i < localTfIdfs.size(); i++) {
	      //System.out.println((SparseVector)localTfIdfs.get(i));
	      inputList.add(new IndexedRow(i,(SparseVector)localTfIdfs.get(i)));
	    }	    
	    // Create a RowMatrix from an JavaRDD<Vector>
        JavaRDD<IndexedRow> rows = sc.parallelize(inputList);

        IndexedRowMatrix imat = new IndexedRowMatrix(rows.rdd());	    
	    IndexedRowMatrix dimat = imat.toCoordinateMatrix().transpose().toIndexedRowMatrix();
	    RowMatrix mat = dimat.toRowMatrix();	  	    
	    CoordinateMatrix simsPerfect = mat.columnSimilarities();
	    
	    // save cosine matrix to file
	    simsPerfect.entries().saveAsTextFile(this.matrixFolder);		
	}
	
	public static void main(String[] args) {
	    
    	// an example
		SparkConf conf = new SparkConf();
		
		conf.setAppName("cluster CalVSM")
		//.setMaster("local[*]")
		.set("spark.kryoserializer.buffer.max", "1g")
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
		.set("spark.driver.allowMultipleContexts", "true"); 
    	JavaSparkContext sc =new JavaSparkContext(conf);
    	
    	System.out.println(conf.toDebugString());
		CalVSM calCosineSimilarity = 
				new CalVSM(args[0], args[1],args[2],
						args[3], sc);
		System.out.println(calCosineSimilarity.csvFilePath);
		calCosineSimilarity.cal();
				    
	    // Close the REPL terminal
	    System.exit(0);	    

	}// End of main()	
}
