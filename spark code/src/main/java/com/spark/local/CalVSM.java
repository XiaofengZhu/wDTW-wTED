package com.spark.local;

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
import com.spark.local.CalVSM;
import com.spark.local.ReadDataFromMySQL;
import com.spark.local.StaticClassLibs.*;

import scala.Tuple2;

public class CalVSM {
	
	private JavaSparkContext sc;
	private ReadDataFromMySQL readDataFromMySQL;	
	private String csvFilePath, inputPath, fileNameFolder, matrixFolder;
	private boolean isDB = false;
	
	public CalVSM(SetParameters setParameters, JavaSparkContext sc ){
		this.csvFilePath = setParameters.getCsvFilePath();
		this.inputPath = setParameters.getInputPath();
		this.fileNameFolder = setParameters.getFileNameFolder();
		this.matrixFolder = setParameters.getMatrixFolder();
		this.readDataFromMySQL = 
				new ReadDataFromMySQL(setParameters, sc);
	    this.sc = sc;
	    this.isDB = (this.csvFilePath == null);
	}
	public CalVSM(String csvFilePath, String inputPath, String fileNameFolder, 
			JavaSparkContext sc ){
		this.csvFilePath = csvFilePath;
		this.inputPath = inputPath;
		this.fileNameFolder = fileNameFolder;
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
	    	JavaRDD<String> txtNames_rowRdd = sc.textFile(this.csvFilePath);
	    	
			txtNames_timestamps_rdd = txtNames_rowRdd
			.mapToPair(new string2PairRDD());			
		}
  
		
		// folder files
		JavaPairRDD<String, String> raw_fileNameContentsRDD = sc.wholeTextFiles(this.inputPath);

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
		System.out.println("fileNames.count(): " +fileNames.count());
		
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
		SetParameters setParameters = new SetParameters();	
	    SparkConf conf = new SparkConf();
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    conf.setMaster(setParameters.getMaster());	    
		conf.set("spark.eventLog.enabled", "false");
    	JavaSparkContext sc =new JavaSparkContext(conf);
    	
    	System.out.println(conf.toDebugString());
		CalVSM calCosineSimilarity = 
				new CalVSM(setParameters,sc);
		System.out.println(calCosineSimilarity.csvFilePath);
		//calCosineSimilarity.cal();
				    
	    // Close the REPL terminal
	    System.exit(0);	    

	}// End of main()
}	