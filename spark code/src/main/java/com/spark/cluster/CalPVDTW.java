package com.spark.cluster;

import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.spark.cluster.StaticClassLibs.FilterSmallSimilarities;
import com.spark.cluster.StaticClassLibs.GetPredictedPairSimilarity;
import com.spark.fastdtw.execute.DTW4DocPair;

import scala.Tuple2;

public class CalPVDTW {
	private String inputPath, fileNameFolder, matrixFolder, pvdtwFolder;
	
	public String getInputPath() {
		return inputPath;
	}
	public String getFileNameFolder() {
		return fileNameFolder;
	}  
	public String getMatrixFolder() {
		return matrixFolder;
	}	
	public String getPVDtwFolder() {
		return pvdtwFolder;
	}

	
	public CalPVDTW(String inputPath, String fileNameFolder, String matrixFolder,
			String pvdtwFolder, SparkContext sparkContext, JavaSparkContext sc){ 
		this.inputPath = inputPath;   	
		this.fileNameFolder = fileNameFolder;		
		this.matrixFolder = matrixFolder;
		this.pvdtwFolder = pvdtwFolder;

	}
	
	public static void main(String[] args) {
	    SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setAppName("Document Revision - PVTDW")
	    //.setMaster("local[*]")
		.set("spark.kryoserializer.buffer.max", "1g")
		// maximum 21 GB memory per executer
		// 4096 MB is the maximum allowable memoryOverhead 21504 MB (21 GB) + 4096 MB = 25600 MB (25GB)
		.set("spark.yarn.executor.memoryOverhead", "4096") 
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
	    .set("spark.driver.allowMultipleContexts", "true"); 

	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    CalPVDTW outputDocumentRevisionPairs = 
	    		new CalPVDTW(args[0], args[1], args[2], 
	    				args[3],sparkContext, sc);
	    
	    // Calculate PV-DTW dist
		
		JavaRDD<String> fileNames_lines = sc
				.textFile(outputDocumentRevisionPairs.getFileNameFolder());// , 20000 200 is enough

		final Broadcast<List<String>> fileName_list_broadcast = sc
				.broadcast(fileNames_lines.collect());
	    
		JavaRDD<String> matrix_lines = 
				sc.textFile(outputDocumentRevisionPairs.getMatrixFolder());// , 20000 200 is enough
		JavaPairRDD<String, Double> indexValues = matrix_lines
				.mapToPair(new GetPredictedPairSimilarity());
		
		JavaPairRDD<String, Double> filtered_indexValues = indexValues
				.filter(new FilterSmallSimilarities());

		JavaPairRDD<String, String> doc_parent_rdd = filtered_indexValues
				.mapToPair(new PairFunction <Tuple2<String, Double>, String, String>() {

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, Double> x) throws Exception {
						// broadcast value for fileName_list
						List<String> fileName_list = fileName_list_broadcast.getValue();
						String [] items = x._1().split(",");
						// doc
						String doc = fileName_list.get(Integer.parseInt(items[0]));
						// parent
						String parent = fileName_list.get(Integer.parseInt(items[1]));
						return new Tuple2(doc, parent);
					}
				
		}); 
		//System.out.println("doc_parent_rdd: " +doc_parent_rdd.take(2));
		//-------------------------------------
		// CONVERT RESULTS TO (FILENAME1, vectorizedContent1, FILENAME2, vectorizedContent2) 
		// txt files in the current corpus
		JavaPairRDD<String, String> raw_fileNameContentsRDD = sc
				.wholeTextFiles(outputDocumentRevisionPairs.getInputPath());// , 20000
		
		JavaPairRDD<String, String> fileNameContentsRDD = raw_fileNameContentsRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					
					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> x) throws Exception {
						// base name
						String txt_name = FilenameUtils.getBaseName(x._1())+".txt";
						// content
						return new Tuple2(txt_name, x._2());
					}
					
			});	
		//System.out.println("fileNameContentsRDD: " +fileNameContentsRDD.take(2));		
		fileNameContentsRDD.cache();
		
		JavaPairRDD<String, Tuple2<String, String>> doc_parent_docContent = doc_parent_rdd
				.join(fileNameContentsRDD);
		
		JavaPairRDD<String, Tuple2<String, String>> parent_doc_docContent_rdd = doc_parent_docContent
				.mapToPair(new SwapKey2Parent());
		
		JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> parent_docContent_parentContent_rdd = 
				parent_doc_docContent_rdd.join(fileNameContentsRDD);
		
		JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> docContent_parentContent_rdd = 
				parent_docContent_parentContent_rdd
				.mapToPair(new ConvertKey2DocTuple());	

		
		//------------------------------------- 
			
		// DTW distance for all doc pairs
		JavaPairRDD<String, Double> revised_old_distance_RDD = docContent_parentContent_rdd
				.mapToPair(new GetDocPairSimilarity());
		revised_old_distance_RDD.saveAsTextFile(outputDocumentRevisionPairs.getPVDtwFolder());	
		// end of cal DTW dist
		
	    // Close the REPL terminal
	    System.exit(0);	
	}// End of main()

	private static class SwapKey2Parent implements 
		PairFunction<Tuple2<String, Tuple2<String, String>>, 
		String, Tuple2<String, String>> {
			
		/**
		 * 
		*/
		private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<String, String>> call(
					Tuple2<String, Tuple2<String, String>> x) throws Exception {
				// parent
				String parent =  x._2()._1();
				// doc content
				String docContent = x._2()._2();							
				return new Tuple2<String, Tuple2<String, String>>
					(parent, new Tuple2<String, String>(x._1(), docContent));
			}
	};

	private static class ConvertKey2DocTuple implements 
		PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, String>>, 
		Tuple2<String, String>, Tuple2<String, String>> {
	
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Tuple2<String, String>, Tuple2<String, String>> call(
					Tuple2<String, Tuple2<Tuple2<String, String>, String>> x) throws Exception {
				// doc
				String doc = x._2()._1()._1();							
				// doc content
				String docContent = x._2()._1()._2();
				
				// parent
				String parent = x._1();
				// parent content
				String parentContent =  x._2()._2();							
				return new Tuple2(new Tuple2(doc, docContent), new Tuple2(parent, parentContent));
			}
	};
	
	private static class GetDocPairSimilarity implements 
	PairFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, String, Double>{
		/**
		 * Convert RDD<Row> to PairRDD<String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Double> call(
				Tuple2<Tuple2<String, String>, Tuple2<String, String>> x) throws Exception {
			String revised_doc_fileName = x._1._1;
			String old_doc_fileName =  x._2._1;
			// document content
			String revised_doc_vector_content = x._1._2;		
			String old_doc_vector_content = x._2._2;	
			DTW4DocPair dTW4DocPair = new DTW4DocPair();
			Double revisedDoc_oLdDoc_distance = Double.MAX_VALUE;
			try{
				revisedDoc_oLdDoc_distance = dTW4DocPair
					.getPVDTWDist(revised_doc_vector_content, old_doc_vector_content);
			}catch (Exception e){
				System.err.println("revised_doc_fileName " + revised_doc_fileName 
						+ " old_doc_vector_content " + old_doc_vector_content 
						+ e.getMessage());
			}

			return new Tuple2<String, Double>(revised_doc_fileName + ";" 
			+ old_doc_fileName, revisedDoc_oLdDoc_distance);
	}};
	
}
