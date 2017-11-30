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

import com.spark.cluster.StaticClassLibs.*;
import com.spark.tree.edit.distance.ParseDoc2Tree;
import com.spark.tree.edit.distance.execute.TED4DocPair;

import scala.Tuple2;

public class CalPVTED {

		private String inputPath, fileNameFolder, matrixFolder, pvtedFolder;
		
		public String getInputPath() {
			return inputPath;
		}
		public String getFileNameFolder() {
			return fileNameFolder;
		}  
		public String getMatrixFolder() {
			return matrixFolder;
		}		
		public String getPVTedFolder() {
			return pvtedFolder;
		}

		
		public CalPVTED(String inputPath, String fileNameFolder, String matrixFolder,
				String pvdtwFolder, SparkContext sparkContext, JavaSparkContext sc){ 
			this.inputPath = inputPath;   	
			this.fileNameFolder = fileNameFolder;		
			this.matrixFolder = matrixFolder;
			this.pvtedFolder = pvdtwFolder;

		}
		
	
	

	public static void main(String[] args) {
	    SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setAppName("Document Revision - PVTED")
	    //.setMaster("local[*]")	    
		.set("spark.kryoserializer.buffer.max", "1g")
		.set("spark.yarn.executor.memoryOverhead", "4096")
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
	    .set("spark.driver.allowMultipleContexts", "true"); 

	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    CalPVTED outputDocumentRevisionPairs = 
	    		new CalPVTED(args[0], args[1], args[2], 
	        			 args[3], sparkContext, sc);
	    
	    // Calculate PV-TED dist

		ParseDoc2Tree parseDoc2Tree = new ParseDoc2Tree();
		final Broadcast <ParseDoc2Tree> parseDoc2Tree_broadcast = 
				sc.broadcast(parseDoc2Tree);
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

		//-------------------------------------
		// CONVERT RESULTS TO (FILENAME1, vectorizedContent1, FILENAME2, vectorizedContent2) 
		// txt files in the current corpus
		JavaPairRDD<String, String> raw_fileNameContentsRDD = sc
				.wholeTextFiles(outputDocumentRevisionPairs.getInputPath());//, 20000
		JavaPairRDD<String, String> fileNameContentsRDD = raw_fileNameContentsRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					
					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> x) throws Exception {
						// base name
						String txt_name = FilenameUtils.getBaseName(x._1())+".txt";
						// content
						// replace content with a tree with vectors (paragraph --> word vectors --> tree)
						String content = x._2();
						ParseDoc2Tree parseDoc2Tree = parseDoc2Tree_broadcast.getValue();
						content =  parseDoc2Tree.getDocumentTree(content);
						return new Tuple2(txt_name, content);
					}
					
		});		
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
			
		// TED distance for all doc pairs
		JavaPairRDD<String, Double> revised_old_distance_RDD = docContent_parentContent_rdd
				.mapToPair(new GetDocPairSimilarity());
		System.out.println("revised_old_distance_RDD " + revised_old_distance_RDD.take(2));
		revised_old_distance_RDD.saveAsTextFile(outputDocumentRevisionPairs.getPVTedFolder());	
		// end of cal DTW dist	
	    // Close the REPL terminal
	    System.exit(0);	
	}// End of main()
	
	private static class GetDTWDist implements 
	PairFunction<String, String, Double>{
	/**
	 * 
	*/
	private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> call(String x)
				throws Exception {
			String [] items = x.replace("(", "").replace(")", "").split(",");
			if (items.length ==2){
				// key: doc,parent
				// value: similarity score
				return new Tuple2<String, Double>(items[0], Double.parseDouble(items[1]));
			}else return new Tuple2(0, Double.POSITIVE_INFINITY);
		}
	};

	
	private static class GetDocParentCandidateDist implements
		PairFunction<Tuple2<String, Double>, String, Tuple2<String, Double>> {
	
		/**
		 * 
		*/
		private static final long serialVersionUID = 1L;

			public Tuple2<String, Tuple2<String, Double>> call(
					Tuple2<String, Double> x) throws Exception {
				String [] items = x._1().split(";");
				// doc
				String index = FilenameUtils.getBaseName(items[0]);
				// parent_doc					
				// parent_doc, converted dtw dist
				//String value = FilenameUtils.getBaseName(items[1]) + "," + x._2();
				
				return new Tuple2 <String, Tuple2<String, Double>>(index, 
						new Tuple2<String, Double>(FilenameUtils.getBaseName(items[1]),x._2()));
			}
		
	};	
	
	
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

		public Tuple2<String, Double> call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> x)
				throws Exception {
			String revised_doc_fileName = x._1._1;
			String old_doc_fileName =  x._2._1;
			// document content
			String revised_doc_content = x._1._2;		
			String old_doc_content = x._2._2;	

			TED4DocPair editTreeDistanceTest = new TED4DocPair();
			editTreeDistanceTest.init();
			Double revisedDoc_oLdDoc_distance;
//			try{			
//				revisedDoc_oLdDoc_distance= editTreeDistanceTest.getEditTreeDistance(revised_doc_content, 
//						old_doc_content);
//			}catch (Exception e){
//				revisedDoc_oLdDoc_distance = Double.POSITIVE_INFINITY;
//			}
			revisedDoc_oLdDoc_distance= editTreeDistanceTest.getEditTreeDistance(revised_doc_content, 
					old_doc_content);				
			return new Tuple2<String, Double>(revised_doc_fileName + ";" + old_doc_fileName, revisedDoc_oLdDoc_distance);
	}};	
}
