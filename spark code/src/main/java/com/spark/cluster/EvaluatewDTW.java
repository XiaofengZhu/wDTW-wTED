package com.spark.cluster;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;

import scala.Tuple2;

import com.spark.cluster.EvaluatewTED.SplitWords;
import com.spark.cluster.StaticClassLibs.FilterSmallSimilarities;
import com.spark.cluster.StaticClassLibs.GetPredictedPairSimilarity;
import com.spark.fastdtw.execute.DTW4DocPair;
import com.spark.word2vec.ParagraphVector;

public class EvaluatewDTW {
	private String inputPath, fileNameFolder, matrixFolder, 
	dtwFolder, word2vecFolder;
	
	private boolean hasWord2vec;
	
	public String getInputPath() {
		return inputPath;
	}
	public String getMatrixFolder() {
		return matrixFolder;
	}
	public String getWord2vecFolder() {
		return word2vecFolder;
	}
	public String getFileNameFolder() {
		return fileNameFolder;
	}    
	public String getDtwFolder() {
		return dtwFolder;
	}

	public boolean getHasWord2vec(){
		return hasWord2vec;
	 }
	
	public EvaluatewDTW(String inputPath, String fileNameFolder, String matrixFolder, 
			String dtwFolder, String word2vecFolder,
			SparkContext sparkContext, JavaSparkContext sc){ 
		this.inputPath = inputPath;   	
		this.fileNameFolder = fileNameFolder;		
		this.matrixFolder = matrixFolder;
		this.dtwFolder = dtwFolder;
		this.word2vecFolder = word2vecFolder;
		this.hasWord2vec = false;


	}

	public EvaluatewDTW(String inputPath, String fileNameFolder, String matrixFolder, 
			String dtwFolder, String word2vecFolder, boolean hasWord2vec,
			SparkContext sparkContext, JavaSparkContext sc){ 
		this.inputPath = inputPath;   	
		this.fileNameFolder = fileNameFolder;		
		this.matrixFolder = matrixFolder;
		this.dtwFolder = dtwFolder;
		this.word2vecFolder = word2vecFolder;
		this.hasWord2vec = hasWord2vec;


	}
	
	public EvaluatewDTW(String inputPath, String fileNameFolder, String matrixFolder, 
			String dtwFolder, String word2vecFolder, String database, String username, String password, 
			String field1, String field2, String tablename, 
			SparkContext sparkContext, JavaSparkContext sc){ 
		this.inputPath = inputPath;   	
		this.fileNameFolder = fileNameFolder;		
		this.matrixFolder = matrixFolder;
		this.dtwFolder = dtwFolder;
		this.word2vecFolder = word2vecFolder;
		this.hasWord2vec = false;


	}

	public EvaluatewDTW(String inputPath, String fileNameFolder, String matrixFolder, 
			String dtwFolder, String word2vecFolder, boolean hasWord2vec,
			String database, String username, String password, 
			String field1, String field2, String tablename, 
			SparkContext sparkContext, JavaSparkContext sc){ 
		this.inputPath = inputPath;   	
		this.fileNameFolder = fileNameFolder;		
		this.matrixFolder = matrixFolder;
		this.dtwFolder = dtwFolder;
		this.word2vecFolder = word2vecFolder;
		this.hasWord2vec = hasWord2vec;


	}
	

	
	public boolean calSign(List<Tuple2<String, Integer>>threshld_count_list, int index){
		Tuple2<String, Integer> t = threshld_count_list.get(index);
		Tuple2<String, Integer> t_large = threshld_count_list.get(index + 1);
		if (t_large._2() > t._2()) return true;
		return false;
	}
	
	public double findDeri(List<Tuple2<String, Integer>>threshld_count_list){
		double dtw_threshold= Double.POSITIVE_INFINITY;
		int threshld_list_size= threshld_count_list.size();
		if (threshld_count_list.size()> 3){
			int index_small = 0;
			int index_large = threshld_list_size - 1;
			int index_mid = (int) (0.5 *(index_small + index_large));
			while (index_small <= 0.5 * threshld_list_size && 
					index_large >= 0.5 * threshld_list_size){
					
					Tuple2<String, Integer> t_s = threshld_count_list.get(index_small);
					Tuple2<String, Integer> t_l = threshld_count_list.get(index_large);
					Tuple2<String, Integer> t_m = threshld_count_list.get(index_mid);
					
					double tem_threshold = Double.parseDouble(t_m._1());
				
					if ((t_m._2() - t_s._2()) * (t_l._2() - t_m._2()) <= 0){
						dtw_threshold = tem_threshold;
						if (calSign(threshld_count_list, index_mid) == 
								calSign(threshld_count_list, index_small))
							index_small = index_mid;
						else
							index_large = index_mid;
					}else{
						break;
					}				
			}			
		}
		return dtw_threshold;
	}
	
	public static void main(String[] args) {
	    SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setAppName("Document Revision - wTDW")
	    .setMaster("local[*]")
		.set("spark.kryoserializer.buffer.max", "1g")
		.set("spark.yarn.executor.memoryOverhead", "4096")		
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
	    .set("spark.driver.allowMultipleContexts", "true"); 
	
	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    EvaluatewDTW outputDocumentRevisionPairs = 
	    		new EvaluatewDTW(args[0], args[1], args[2], 
	        			args[3], args[4], true, sparkContext, sc);
	    
	    // Calculate DTW dist
		
		Word2VecModel model;
		if (outputDocumentRevisionPairs.getHasWord2vec()){
			System.out.println("Using word2vec model from " +
					outputDocumentRevisionPairs.getWord2vecFolder());
			model = Word2VecModel.load(sparkContext, 
					outputDocumentRevisionPairs.getWord2vecFolder());
		}else{
			JavaRDD<List<String>> corpusLarge_txtNames_rowRdd;
			if (outputDocumentRevisionPairs.getInputPath().contains("wiki")) {
			    corpusLarge_txtNames_rowRdd = 
			    		sc.textFile(outputDocumentRevisionPairs.getInputPath())//, 20000
			    					.map(new SplitWords()); 					
			} else {
			    corpusLarge_txtNames_rowRdd = 
			    		sc.textFile(outputDocumentRevisionPairs.getInputPath()
			    				.substring(0, outputDocumentRevisionPairs.getInputPath().length()-1) + "5")
			    					.map(new SplitWords()); 				
			}
		    Word2Vec word2vec = new Word2Vec()
		  	      .setVectorSize(10)
		  	      .setSeed(42L);
	  	    model = word2vec.fit(corpusLarge_txtNames_rowRdd);    
//		    model.save(sparkContext, 
//		    		outputDocumentRevisionPairs.getWord2vecFolder());			
		}
		
		// replace words with trained vectors
		ParagraphVector paragraphVector = new ParagraphVector(sparkContext, model);
		final Broadcast <ParagraphVector> paragraphVector_broadcast = 
				sc.broadcast(paragraphVector);
		
		JavaRDD<String> fileNames_lines = sc
				.textFile(outputDocumentRevisionPairs.getFileNameFolder());// , 20000 1000 is enough
		final Broadcast<List<String>> fileName_list_broadcast = sc
				.broadcast(fileNames_lines.collect());
	    
		JavaRDD<String> matrix_lines = 
				sc.textFile(outputDocumentRevisionPairs.getMatrixFolder());// , 20000 1000 is enough
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
				.wholeTextFiles(outputDocumentRevisionPairs.getInputPath(), 20000);
		
		JavaPairRDD<String, String> fileNameContentsRDD = raw_fileNameContentsRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					
					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> x) throws Exception {
						ParagraphVector paragraphVector = paragraphVector_broadcast.getValue();
						// base name
						String txt_name = FilenameUtils.getBaseName(x._1())+".txt";
						// content
						// replace content with a tree with vectors (paragraph --> word vectors --> tree)
						String content = paragraphVector.replaceWordWithVector(x._2());
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
			
		// DTW distance for all doc pairs
		JavaPairRDD<String, Double> revised_old_distance_RDD = docContent_parentContent_rdd
				.mapToPair(new GetDocPairSimilarity());
		revised_old_distance_RDD.saveAsTextFile(outputDocumentRevisionPairs.getDtwFolder());	
		// end of cal DTW dist
		
	
	    // Close the REPL terminal
	    System.exit(0);	
	}// End of main()
	
	
	public static class SplitWords implements Function<String, List<String>> {
		    public List<String> call(String s) {
			      return Arrays.asList(s.split(" "));
	}};
	
//	public static class FilterDTWHighDistance implements Function <Tuple2<String, Double>, Boolean> {
//		/**
//		 * PairRDD <String, Double> to PairRDD <String, Double>
//		 */
//		private static final long serialVersionUID = 1L;
//
//		public Boolean call(Tuple2<String, Double> s) { if (s._2() < dtw_threshold) return true; return false; }
//	};
	
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
			Double revisedDoc_oLdDoc_distance;
//			try{
//				revisedDoc_oLdDoc_distance = dTW4DocPair
//					.getDTWDist(revised_doc_vector_content, old_doc_vector_content);
//			}catch (Exception e){
//				revisedDoc_oLdDoc_distance = Double.POSITIVE_INFINITY;
//			}
			revisedDoc_oLdDoc_distance = dTW4DocPair
					.getDTWDist(revised_doc_vector_content, old_doc_vector_content);

			return new Tuple2<String, Double>(revised_doc_fileName + ";" 
			+ old_doc_fileName, revisedDoc_oLdDoc_distance);
	}};
	
}
