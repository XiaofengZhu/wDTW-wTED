package com.spark.cluster;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;

import com.spark.cluster.StaticClassLibs.*;
import com.spark.fastdtw.execute.DTW4DocPair;
import com.spark.word2vec.ParagraphVector;

import scala.Tuple2;

public class ReEvaluateePVDTW {
	private String matrixFolder, outputFolder;

	public String getMatrixFolder() {
		return matrixFolder;
	}

	public String getOutputFolder() {
		return outputFolder;
	}

	// dtw tao
	private static double dtw_threshold = 22000.00; // Double.POSITIVE_INFINITY
	
	public ReEvaluateePVDTW(String matrixFolder, String outputFolder, 
			SparkContext sparkContext, JavaSparkContext sc){ 	
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
	    .setAppName("Document Revision - PV-TDW")
	    .set("spark.driver.allowMultipleContexts", "true"); 
	    conf.setMaster("local[*]");	
	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    ReEvaluateePVDTW outputDocumentRevisionPairs = 
	    		new ReEvaluateePVDTW(args[0], args[1], sparkContext, sc);
	    
		JavaRDD<String> matrix_lines = sc.textFile(outputDocumentRevisionPairs.getMatrixFolder());
		JavaPairRDD<String, Double> indexValues = matrix_lines
				.mapToPair(new PairFunction<String, String, Double>(){
					@Override
					public Tuple2<String, Double> call(String x)
							throws Exception {
						String [] items = x.replace("(", "").replace(")", "").split(",");
						if (items.length ==2){
							// key: doc,parent
							// value: similarity score
							return new Tuple2(items[0], Double.parseDouble(items[1]));
						}else return new Tuple2("NULL NULL", Double.POSITIVE_INFINITY);
				}});	    
		
    	/*
		 * Estimate the optimal tau
		 */
		
//		final Accumulator<Double> dist_sum = sc.accumulator(0.0);	
//		JavaRDD<Double> filteredNumbers = indexValues.map(
//				new Function<Tuple2<String, Double>, Double>() {
//				private static final long serialVersionUID = 1L;
//	
//				public Double call(Tuple2<String, Double> s) { 
//					dist_sum.add(s._2());
//					return s._2(); 
//				}
//		});
//		//System.err.println("filteredNumbers " + filteredNumbers.take(3));
//	    //System.err.println("dist sum is " + dist_sum.value());
//	    //final Broadcast<Double> dist_sum_broadcast = sc.broadcast(dist_sum.value());
//	    JavaRDD<String> formattedNumber_string = filteredNumbers.map( 
//	    		new Function<Double, String>() {
//	
//				public String call(Double d) { 
//					//d = d / (Double) dist_sum_broadcast.value();
//					DecimalFormat df = new DecimalFormat("#.#");
//					return df.format(d);
//				}
//		});
//	    System.err.println("formattedNumber_string " + formattedNumber_string.take(3));
//	    
//		// calculate dist distribution
//		
//		JavaPairRDD<String, Integer> threshold_counts = formattedNumber_string
//				.mapToPair(new AddCountOne())
//				.reduceByKey(new AddUpOnes())
//				.sortByKey(); // ascending 0 --> 1
//		threshold_counts.saveAsTextFile(outputDocumentRevisionPairs.getOutputFolder()+"_threshold");
		/*
		System.err.println("threshold_counts " + threshold_counts.take(3));		
		List<Tuple2<String, Integer>>threshld_count_list = threshold_counts.collect();
		
		dtw_threshold = outputDocumentRevisionPairs.findDeri(threshld_count_list);
		*/
		System.err.println("dtw_threshold is " + dtw_threshold);
		final Broadcast<Double> dtw_threshold_broadCast = sc.broadcast(dtw_threshold);		
		// End of estimating the optimal tau
		
		JavaPairRDD<String, Double> dtw_filtered_indexValues;
		if (dtw_threshold < Double.POSITIVE_INFINITY){
			dtw_filtered_indexValues = indexValues
					.filter(new Function <Tuple2<String, Double>, Boolean> (){
						/**
						 * PairRDD <String, Double> to PairRDD <String, Double>
						 */
						private static final long serialVersionUID = 1L;

						public Boolean call(Tuple2<String, Double> s) { 
							if (s._2() <= dtw_threshold_broadCast.value()) 
								return true; 
							return false; }
			});			
		}else{
			dtw_filtered_indexValues = indexValues;
		}
		

		JavaPairRDD<String, Tuple2<String, Double>> doc_parentDist_rdd = dtw_filtered_indexValues
				.mapToPair(new GetDocParentCandidateDist());
		System.err.println("doc_parent_rdd " + doc_parentDist_rdd.take(2));
		
		JavaPairRDD<String, Tuple2<String, Double>> result_rdd = doc_parentDist_rdd
				.reduceByKey(new DTWGetDocParent());
		
		String period = args[2]; // "2"
		//String output_folder = "." + File.separator + "output"+period+"_"+ vsm_threshold +"_vsm";
		int my_revision_count = 0;
		int true_positive_count = 0;
		int true_revision_count = Integer.parseInt(args[3]);//should be the number of revised documents	265 		
		List<Tuple2<String, Tuple2<String, Double>>>  doc_parent_list = result_rdd.collect();
		List<Tuple2<String, String>> result_tuple2_rdd = new ArrayList<Tuple2<String, String>>();
		for (Tuple2<String, Tuple2<String, Double>> doc_parent : doc_parent_list){
			
			String doc = FilenameUtils.getBaseName(doc_parent._1());
			String parent_doc = FilenameUtils.getBaseName(doc_parent._2()._1());
			//System.out.println(doc +" -- "+ parent_doc);
			if (doc.startsWith(period.substring(period.length()-1, period.length())+"_") ){//
				if (doc.contains(parent_doc)) {
					true_positive_count++;
					System.out.println(doc +" ---- "+ parent_doc);
				}
			result_tuple2_rdd.add(new Tuple2(doc, parent_doc));
			my_revision_count++;				
			}
		}
		
	    // Evaluation  
		double precision = (double)true_positive_count/my_revision_count;
		double recall = (double)true_positive_count/true_revision_count;
		double f_measure = 2 * precision * recall/ (precision + recall);
		System.err.println("true_positive_count:"+true_positive_count);
		System.err.println("my_revision_count: "+my_revision_count);
		System.err.println("true_revision_count: "+true_revision_count);
		System.out.println("precision-dtw_threshold-"+dtw_threshold+": " +precision);
		System.out.println("recall-dtw_threshold-"+dtw_threshold+": " +recall);	
		System.out.println("f-measure-dtw_threshold-"+dtw_threshold+": " +f_measure);
		
		ArrayList <Double> values = new ArrayList<Double>();
		values.add(precision);
		values.add(recall);
		values.add(f_measure);
		JavaRDD<Double> values_rdd = sc.parallelize(values);
		//JavaRDD<Tuple2<String, String>> correct_result_rdd = sc.parallelize(result_tuple2_rdd);
		values_rdd.saveAsTextFile(outputDocumentRevisionPairs.getOutputFolder());			
		
		//System.err.println("result_rdd" + correct_result_rdd.take(2));
		//correct_result_rdd.saveAsTextFile(outputDocumentRevisionPairs.getOutputFolder());		
	    // Close the REPL terminal
	    System.exit(0);	
	}// End of main()

	
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
	
	
	public static class SplitWords implements Function<String, List<String>> {
		    public List<String> call(String s) {
			      return Arrays.asList(s.split(" "));
	}};
	
	public static class FilterDTWHighDistance implements Function <Tuple2<String, Double>, Boolean> {
		/**
		 * PairRDD <String, Double> to PairRDD <String, Double>
		 */
		private static final long serialVersionUID = 1L;

		public Boolean call(Tuple2<String, Double> s) { if (s._2() < dtw_threshold) return true; return false; }
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

		public Tuple2<String, Double> call(
				Tuple2<Tuple2<String, String>, Tuple2<String, String>> x) throws Exception {
			String revised_doc_fileName = x._1._1;
			String old_doc_fileName =  x._2._1;
			// document content
			String revised_doc_vector_content = x._1._2;		
			String old_doc_vector_content = x._2._2;	
			DTW4DocPair dTW4DocPair = new DTW4DocPair();
			Double revisedDoc_oLdDoc_distance;
			try{
				revisedDoc_oLdDoc_distance = dTW4DocPair
					.getDTWDist(revised_doc_vector_content, old_doc_vector_content);
			}catch (Exception e){
				revisedDoc_oLdDoc_distance = Double.POSITIVE_INFINITY;
			}


			return new Tuple2<String, Double>(revised_doc_fileName + ";" 
			+ old_doc_fileName, revisedDoc_oLdDoc_distance);
	}};
	
}
