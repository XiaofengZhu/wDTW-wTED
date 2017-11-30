package com.spark.cluster;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.spark.cluster.StaticClassLibs.*;

import scala.Tuple2;

public class PreCalVSM {
	private String fileNameFolder, matrixFolder, outputFolder;
	
	public String getFileNameFolder() {
		return fileNameFolder;
	}
	
	public String getMatrixFolder() {
		return matrixFolder;
	}

	public String getOutputFolder() {
		return outputFolder;
	}

	// private CalVSM calVSM;
    // vsm tao
    private static double vsm_threshold = 0.6;
    


	public PreCalVSM(String fileNameFolder, 
			String matrixFolder, String outputFolder, 
			SparkContext sparkContext, JavaSparkContext sc){ 
		
	    this.fileNameFolder = fileNameFolder;
	    this.matrixFolder = matrixFolder;
	    this.outputFolder = outputFolder;
	}

	public boolean calSign(List<Tuple2<String, Integer>>threshld_count_list, int index){
		Tuple2<String, Integer> t = threshld_count_list.get(index);
		Tuple2<String, Integer> t_large = threshld_count_list.get(index + 1);
		if (t_large._2() > t._2()) return true;
		return false;
	}
	
	public double findDeri(List<Tuple2<String, Integer>>threshld_count_list){
		int threshld_list_size= threshld_count_list.size();
		if (threshld_count_list.size()> 3){
			int index_small = 0;
			int index_large = threshld_list_size-1;
			int index_mid = (int) (0.5 *(index_small + index_large));
			while (index_small <= 0.5 * threshld_list_size && index_large >= 0.5 * threshld_list_size){
					
					Tuple2<String, Integer> t_s = threshld_count_list.get(index_small);
					Tuple2<String, Integer> t_l = threshld_count_list.get(index_large);
					Tuple2<String, Integer> t_m = threshld_count_list.get(index_mid);
					
					double tem_threshold = Double.parseDouble(t_m._1());
				
					if ((t_m._2() - t_s._2()) * (t_l._2() - t_m._2()) <= 0){
						vsm_threshold = tem_threshold;
						if (calSign(threshld_count_list, index_mid) == 
								calSign(threshld_count_list, index_small))
							index_small = index_mid;
						else
							index_large = index_mid;
					}else{
						break;
					}
					
			}
			System.err.println("vsm_threshold is " + vsm_threshold);
		}
		return vsm_threshold;
	}
	
	public static void main(String[] args) {
	    SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setAppName("Document Revision - VSM")
	    //.setMaster("local[*]")
		.set("spark.kryoserializer.buffer.max", "1g")
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
	    .set("spark.driver.allowMultipleContexts", "true"); 
	
	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    PreCalVSM vsmOutputDocumentRevisionPairs = 
	    		new PreCalVSM(args[0], args[1], args[2],
						sparkContext, sc);
	    
	    // Calculate VSM dist
	    // vsmOutputDocumentRevisionPairs.cal();
	    
		JavaRDD<String> fileNames_lines = sc
				.textFile(vsmOutputDocumentRevisionPairs.getFileNameFolder());// ,20000 is not required
		final Broadcast<List<String>> fileName_list_broadcast = sc
				.broadcast(fileNames_lines.collect());		
	    
		JavaRDD<String> matrix_lines = sc
				.textFile(vsmOutputDocumentRevisionPairs.getMatrixFolder());// ,20000 is not required
		JavaPairRDD<String, Double> indexValues = matrix_lines
				.mapToPair(new GetPredictedPairSimilarity());
		JavaPairRDD<String, String> doc_parentDist_rdd = indexValues
				.mapToPair(new GetDocParentCandidateDist());
		JavaPairRDD<String, String> docParent_dist_rdd = doc_parentDist_rdd
				.mapToPair(new PairFunction <Tuple2<String, String>, String, String>() {

			@Override
			public Tuple2<String, String> call(
					Tuple2<String, String> x) throws Exception {
				// broadcast value for fileName_list
				List<String> fileName_list = fileName_list_broadcast.getValue();
				// doc
				String doc = fileName_list.get(Integer.parseInt(x._1()));
				// parent score
				String [] parent_score = x._2().split(",");
				// parent
				String parent = fileName_list.get(Integer.parseInt(parent_score[0]));
				String doc_parent = doc +  ";" + parent;
				return new Tuple2(doc_parent, parent_score[1]);
			}
		
		});			
		docParent_dist_rdd.saveAsTextFile(vsmOutputDocumentRevisionPairs.getOutputFolder());	

	    // Close the REPL terminal
	    System.exit(0);	 
	}// End of main()
	 
	private static class GetDocParentCandidateDist implements
		PairFunction<Tuple2<String, Double>, String, String> {
	
		/**
		 * 
		*/
		private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(
					Tuple2<String, Double> x) throws Exception {
				String [] items = x._1().split(",");
				// doc
				String index = FilenameUtils.getBaseName(items[0]);
				// parent_doc					
				// parent_doc, converted score
				String value = FilenameUtils.getBaseName(items[1]) + "," + x._2();
				return new Tuple2 <String, String>(index, value);
			}
		
	};	
	
}

