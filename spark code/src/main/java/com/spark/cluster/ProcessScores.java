package com.spark.cluster;

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

import com.spark.cluster.StaticClassLibs.DTWGetDocParent;

import scala.Tuple2;

public class ProcessScores {
	private static double cutoff_threshold = 0.6; 
	private String scoreFolder, scoreCountFolder, outputFolder;

	public String getScoreFolder() {
		return scoreFolder;
	}
	public String getScoreCountFolder() {
		return scoreCountFolder;
	}
	public String getOutputFolder() {
		return outputFolder;
	}	
	public ProcessScores(String scoreFolder, String scoreCountFolder, String outputFolder) {
		this.scoreFolder = scoreFolder;
		this.scoreCountFolder = scoreCountFolder;
		this.outputFolder = outputFolder;
	}

	 public double findDeri(List<Tuple2<String, Integer>>threshld_count_list){
		double ted_threshold= Double.POSITIVE_INFINITY;
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
						ted_threshold = tem_threshold;
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
		return ted_threshold;
	}
		public boolean calSign(List<Tuple2<String, Integer>>threshld_count_list, int index){
			Tuple2<String, Integer> t = threshld_count_list.get(index);
			Tuple2<String, Integer> t_large = threshld_count_list.get(index + 1);
			if (t_large._2() > t._2()) return true;
			return false;
		}
		
	public static void main(String[] args) {
	    SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setAppName("Document Revision - process score")
	    .setMaster("local[*]")	    
		.set("spark.kryoserializer.buffer.max", "1g")
		.set("spark.yarn.executor.memoryOverhead", "4096")		
	    .set("spark.executor.heartbeatInterval", "500000s")
	    .set("spark.network.timeout", "10000000s")
	    .set("spark.driver.allowMultipleContexts", "true"); 
	
	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    ProcessScores processScore = new ProcessScores(args[0], args[1], args[2]);
		JavaRDD<String> docParent_score_lines = sc.textFile(processScore.getScoreFolder());
		JavaPairRDD<String, Double> docParent_score_rdd = docParent_score_lines
				.mapToPair(new GetDocParentDist());  
		
    	/*
		 * Estimate the optimal tau
		 */
		/*
		final Accumulator<Double> dist_sum = sc.accumulator(0.0);	
		JavaRDD<Double> filteredNumbers = docParent_score_rdd.map(
				new Function<Tuple2<String, Double>, Double>() {
				private static final long serialVersionUID = 1L;
	
				public Double call(Tuple2<String, Double> s) { 
					dist_sum.add(s._2());
					return s._2(); 
				}
		});
		//System.err.println("filteredNumbers " + filteredNumbers.take(3));
	    //System.err.println("dist sum is " + dist_sum.value());
	    //final Broadcast<Double> dist_sum_broadcast = sc.broadcast(dist_sum.value());
		
		// calculate score: count
	    JavaRDD<String> formattedNumber_string = filteredNumbers.map( 
	    		new Function<Double, String>() {
	
				public String call(Double d) { 
					//d = d / (Double) dist_sum_broadcast.value();
					DecimalFormat df = new DecimalFormat("#.##");
					return df.format(d);
				}
		});
	    System.err.println("formattedNumber_string " + formattedNumber_string.take(3));
	    
		// calculate dist distribution
		
		JavaPairRDD<String, Integer> threshold_counts = formattedNumber_string
				.mapToPair(new AddCountOne())
				.reduceByKey(new AddUpOnes())
				.sortByKey(); // ascending 0 --> 1
		threshold_counts.saveAsTextFile(processScore.getScoreCountFolder());
		
		
		System.err.println("threshold_counts " + threshold_counts.take(3));		
		List<Tuple2<String, Integer>>threshld_count_list = threshold_counts.collect();
		
		cutoff_threshold = processScore.findDeri(threshld_count_list);		
		*/
		
		System.out.println("cutoff_threshold is " + cutoff_threshold);
		final Broadcast<Double> cutoff_threshold_broadCast = sc.broadcast(cutoff_threshold);		
		// End of estimating the optimal tau
		
		JavaPairRDD<String, Double> dtw_filtered_indexValues;
		if (processScore.getScoreFolder().contains("vsm") || processScore.getScoreFolder().contains("awe")){
			dtw_filtered_indexValues = docParent_score_rdd
					.filter(new Function <Tuple2<String, Double>, Boolean> (){
						/**
						 * PairRDD <String, Double> to PairRDD <String, Double>
						 */
						private static final long serialVersionUID = 1L;

						public Boolean call(Tuple2<String, Double> s) { 
							if (s._2() >= cutoff_threshold_broadCast.value()) 
								return true; 
							return false; 
							}
					});			
		}else{
			dtw_filtered_indexValues = docParent_score_rdd
					.filter(new Function <Tuple2<String, Double>, Boolean> (){
						/**
						 * PairRDD <String, Double> to PairRDD <String, Double>
						 */
						private static final long serialVersionUID = 1L;

						public Boolean call(Tuple2<String, Double> s) { 
							if (s._2() <= cutoff_threshold_broadCast.value()) 
								return true; 
							return false; 
							}
					});	
		}
		

		JavaPairRDD<String, Tuple2<String, Double>> doc_parentDist_rdd = dtw_filtered_indexValues
				.mapToPair(new GetDocParentCandidateDist());
		System.err.println("doc_parent_rdd " + doc_parentDist_rdd.take(2));
		
		JavaPairRDD<String, Tuple2<String, Double>> result_rdd = doc_parentDist_rdd
				.reduceByKey(new DTWGetDocParent());
		
		String period = args[3]; // "2", "small"
		int my_revision_count = 0;
		int true_positive_count = 0;
		int true_revision_count = Integer.parseInt(args[4]);//should be the number of revised documents	265 
		
		List<Tuple2<String, Tuple2<String, Double>>>  doc_parent_list = result_rdd.collect();
		//List<Tuple2<String, String>> result_tuple2_rdd = new ArrayList<Tuple2<String, String>>();
		
		if (period.contains("wiki")){// wiki corpus
			for (Tuple2<String, Tuple2<String, Double>> doc_parent : doc_parent_list){
				
				String doc = FilenameUtils.getBaseName(doc_parent._1());
				String parent_doc = FilenameUtils.getBaseName(doc_parent._2()._1());
				//result_tuple2_rdd.add(new Tuple2(doc, parent_doc));
				System.out.println("prediction: " + doc +" -- "+ parent_doc);
				
				doc = doc.substring(0, doc.indexOf("_"));
				int doc_id = Integer.parseInt(doc.substring(doc.indexOf("_") + 1, doc.length()));
				parent_doc = parent_doc.substring(0, parent_doc.indexOf("_"));
				int parent_id = Integer.parseInt(parent_doc.substring(parent_doc.indexOf("_") + 1, parent_doc.length()));
				
				if (doc.equals(parent_doc) && doc_id > parent_id) {
					true_positive_count++;
					System.out.println("correct prediction: " + doc +" ---- "+ parent_doc);
				}
				my_revision_count++;				
			}			
		} else {
			for (Tuple2<String, Tuple2<String, Double>> doc_parent : doc_parent_list){
				
				String doc = FilenameUtils.getBaseName(doc_parent._1());
				String parent_doc = FilenameUtils.getBaseName(doc_parent._2()._1());
				System.out.println(doc +" -- "+ parent_doc);
				if (doc.startsWith(period.substring(period.length()-1, period.length())+"_") ){//
					if (doc.contains(parent_doc)) {
						true_positive_count++;
						System.out.println(doc +" ---- "+ parent_doc);
					}
				//result_tuple2_rdd.add(new Tuple2(doc, parent_doc));
				my_revision_count++;				
				}
			}			
		}
		
	    // Evaluation  
		double precision = (double)true_positive_count/my_revision_count;
		double recall = (double)true_positive_count/true_revision_count;
		double f_measure = 2 * precision * recall/ (precision + recall);
		System.err.println("true_positive_count:"+true_positive_count);
		System.err.println("my_revision_count: "+my_revision_count);
		System.err.println("true_revision_count: "+true_revision_count);
		System.out.println("precision-cutoff_threshold-"+cutoff_threshold+": " +precision);
		System.out.println("recall-cutoff_threshold-"+cutoff_threshold+": " +recall);	
		System.out.println("f-measure-cutoff_threshold-"+cutoff_threshold+": " +f_measure);
		
		ArrayList <Double> values = new ArrayList<Double>();
		values.add(precision);
		values.add(recall);
		values.add(f_measure);
		JavaRDD<Double> values_rdd = sc.parallelize(values);
		values_rdd.saveAsTextFile(processScore.getOutputFolder());	
		
		//JavaRDD<Tuple2<String, String>> correct_result_rdd = sc.parallelize(result_tuple2_rdd);		
		//System.err.println("result_rdd" + correct_result_rdd.take(2));
		//correct_result_rdd.saveAsTextFile(outputDocumentRevisionPairs.getOutputFolder());	
	}
	
	private static class GetDocParentDist implements 
	PairFunction<String, String, Double>{
	/**
	 * 
	*/
	private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> call(String x)
				throws Exception {
			x = x.replace("(", "").replace(")", "");
			int i = x.lastIndexOf(",");
			String[] items =  {x.substring(0, i), x.substring(i + 1)};
			if (items.length ==2){
				// key: doc;parent
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
	
}
