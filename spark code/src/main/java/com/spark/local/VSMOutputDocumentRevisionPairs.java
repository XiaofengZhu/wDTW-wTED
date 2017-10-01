package com.spark.local;

import java.io.File;
import java.util.ArrayList;
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

import com.spark.local.StaticClassLibs.*;

import scala.Tuple2;

public class VSMOutputDocumentRevisionPairs {
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

	private CalVSM calVSM;
    // vsm tao
    private static double vsm_threshold = 0.5;
    


	public VSMOutputDocumentRevisionPairs(SetParameters setParameters,
									   SparkContext sparkContext, JavaSparkContext sc){ 
		
	    this.fileNameFolder = setParameters.getFileNameFolder();
	    this.matrixFolder = setParameters.getMatrixFolder();
	    this.outputFolder = setParameters.getOutputFolder();
	    this.calVSM = 
				new CalVSM(setParameters, sc);
	}
	
	public void cal(){
		this.calVSM.cal();
		
	}
	
	public boolean calSign(List<Tuple2<String, Integer>>threshld_count_list, int index){
		Tuple2<String, Integer> t = threshld_count_list.get(index);
		Tuple2<String, Integer> t_large = threshld_count_list.get(index + 1);
		if (t_large._2() > t._2()) return true;
		return false;
	}
	
	public double findDeri(List<Tuple2<String, Integer>>threshld_count_list){
		int threshld_list_size= threshld_count_list.size();
		if (threshld_count_list.size()> 2){
			int index_small = 0;
			int index_large = threshld_list_size-1;
			int index_mid = (int) (0.5 *(index_small + index_large));
			while (index_small <= 0.5 * threshld_list_size && index_large >= 0.5 * threshld_list_size){
					
					Tuple2<String, Integer> t_s = threshld_count_list.get(index_small);
					Tuple2<String, Integer> t_l = threshld_count_list.get(index_large);
					Tuple2<String, Integer> t_m = threshld_count_list.get(index_mid);
					
					double tem_vsm_threshold = Double.parseDouble(t_m._1());
				
					if ((t_m._2() - t_s._2()) * (t_l._2() - t_m._2()) <= 0){
						vsm_threshold = tem_vsm_threshold;
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
		SetParameters setParameters = new SetParameters();	
	    SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads - which represents
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setMaster(setParameters.getMaster())
	    .setAppName("VSM Document Revision")
	    .set("spark.driver.allowMultipleContexts", "true"); 				
	    SparkContext sparkContext = new SparkContext(conf);
	    JavaSparkContext sc = new JavaSparkContext(sparkContext);
	    
	    VSMOutputDocumentRevisionPairs vsmOutputDocumentRevisionPairs = 
	    		new VSMOutputDocumentRevisionPairs(setParameters, sparkContext, sc);
	    
	    // Calculate VSM dist
	    vsmOutputDocumentRevisionPairs.cal();
	    
		JavaRDD<String> fileNames_lines = sc
				.textFile(vsmOutputDocumentRevisionPairs.getFileNameFolder());
		final Broadcast<List<String>> fileName_list_broadcast = sc
				.broadcast(fileNames_lines.collect());		
	    
		JavaRDD<String> matrix_lines = sc
				.textFile(vsmOutputDocumentRevisionPairs.getMatrixFolder());
		JavaPairRDD<String, Double> indexValues = matrix_lines
				.mapToPair(new GetPredictedPairSimilarity());
		
		JavaPairRDD<String, Double> filtered_indexValues = indexValues
				.filter(new FilterSmallSimilarities());		
		
    	/*
		 * Estimate the optimal tau
		 */
		/*
		final Accumulator<Double> score_sum = sc.accumulator(0.0);	
		JavaRDD<Double> filteredNumbers = filtered_indexValues.map(
				new Function<Tuple2<String, Double>, Double>() {
				private static final long serialVersionUID = 1L;
	
				public Double call(Tuple2<String, Double> s) { 
					score_sum.add(s._2());
					return s._2(); 
				}
		});
		System.err.println("filteredNumbers " + filteredNumbers.take(3));
	    System.err.println("dist sum is " + score_sum.value());
	    final Broadcast<Double> score_sum_broadcast = sc.broadcast(score_sum.value());
	    JavaRDD<String> formattedNumber_string = filteredNumbers.map( 
	    		new Function<Double, String>() {
	
				public String call(Double d) { 
					d = d / (Double) score_sum_broadcast.value();
					DecimalFormat df = new DecimalFormat("#.##");
					return df.format(d);
				}
		});
	    //System.err.println("formattedNumber_string " + formattedNumber_string.take(3));
	    
		// calculate dist distribution		
		JavaPairRDD<String, Integer> vsm_threshold_counts = formattedNumber_string
				.mapToPair(new AddCountOne())
				.reduceByKey(new AddUpOnes())
				.sortByKey(); // ascending 0 --> 1
		//System.err.println("vsm_threshold_counts " + vsm_threshold_counts.take(3));		
		List<Tuple2<String, Integer>>threshld_count_list = vsm_threshold_counts.collect();
		
		vsm_threshold = vsmOutputDocumentRevisionPairs.findDeri(threshld_count_list);
		
		*/
		// End of estimating the optimal tau
	    
	    final Broadcast<Double> vsm_threshold_broadCast = sc.broadcast(vsm_threshold);	
		JavaPairRDD<String, Double> vsm_filtered_indexValues = filtered_indexValues
				.filter(new Function <Tuple2<String, Double>, Boolean> (){
					/**
					 * PairRDD <String, Double> to PairRDD <String, Double>
					 */
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Double> s) { 
						if (s._2() >= vsm_threshold_broadCast.value()) 
							return true; 
						return false; }
		});
		//System.out.println("vsm_filtered_indexValues " + vsm_filtered_indexValues.take(5));
		
		JavaPairRDD<String, String> doc_parentDist_rdd = vsm_filtered_indexValues
				.mapToPair(new GetDocParentCandidateDist());
		
		JavaPairRDD<String, String> doc_parent_rdd = doc_parentDist_rdd
				.reduceByKey(new GetDocParent());
		//System.out.println("doc_parent_rdd" + doc_parent_rdd.take(3));
		
		JavaPairRDD<String, String> result_rdd = doc_parent_rdd
				.mapToPair(new PairFunction <Tuple2<String, String>, String, String>() {

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> x) throws Exception {
						// broadcast value for fileName_list
						List<String> fileName_list = fileName_list_broadcast.getValue();
						// doc
						String doc = fileName_list.get(Integer.parseInt(x._1()));
						// parent
						String [] parent_score = x._2().split(",");
						String parent = fileName_list.get(Integer.parseInt(parent_score[0]));
						return new Tuple2(doc, parent);
					}
				
		});	
		System.out.println("result_rdd" + result_rdd.take(3));		
		
//		/*
//		JavaPairRDD<String, String> result_rdd = doc_parent_rdd
//				.mapToPair(new PairFunction <Tuple2<String, String>, String, String>() {
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<String, String> x) throws Exception {
//						// broadcast value for fileName_list
//						List<String> fileName_list = fileName_list_broadcast.getValue();
//						// doc
//						String doc = fileName_list.get(Integer.parseInt(x._1()));
//						// parent
//						String [] parent_score = x._2().split(",");
//						String parent = fileName_list.get(Integer.parseInt(parent_score[0]));
//						return new Tuple2(doc, parent + ", " +parent_score[1]);
//					}
//				
//		});
//		*/
	    
		String period = "1";
		String output_folder = "." + File.separator + "output"+period+"_"+ vsm_threshold +"_vsm";
		int my_revision_count = 0;
		int true_positive_count = 0;
		int true_revision_count = 266;//should be the number of revised documents	288  		
		List<Tuple2<String, String>>  doc_parent_list = result_rdd.collect();
		List<Tuple2<String, String>> result_tuple2_rdd = new ArrayList<Tuple2<String, String>>();
		for (Tuple2<String, String> doc_parent : doc_parent_list){
			
			String doc = FilenameUtils.getBaseName(doc_parent._1());
			String parent_doc = FilenameUtils.getBaseName(doc_parent._2());
			System.out.println(doc +" -- "+ parent_doc);
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
		System.out.println("precision-vsm_threshold-"+vsm_threshold+": " +precision);
		System.out.println("recall-vsm_threshold-"+vsm_threshold+": " +recall);	
		System.out.println("f-measure-vsm_threshold-"+vsm_threshold+": " +f_measure);
		
		ArrayList <Double> values = new ArrayList<Double>();
		values.add(precision);
		values.add(recall);
		values.add(f_measure);
		JavaRDD<Double> values_rdd = sc.parallelize(values);
		JavaRDD<Tuple2<String, String>> correct_result_rdd = sc.parallelize(result_tuple2_rdd);
		values_rdd.saveAsTextFile(output_folder);			
		
	    System.out.println("Results will be saved in " + 
	    		vsmOutputDocumentRevisionPairs.getOutputFolder());
		System.err.println("result_rdd" + correct_result_rdd.take(2));
		correct_result_rdd.saveAsTextFile(vsmOutputDocumentRevisionPairs.getOutputFolder());		    
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
