package com.spark.local;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;

import scala.Tuple2;

public class StaticClassLibs {
	// tf-idf tao
    private final static double threshold  = 0.5;
	public static class Row2PairRDD implements PairFunction<Row, String, String>{
		/**
		 * Convert RDD<Row> to PairRDD<String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(Row x)
				throws Exception {
			String [] row = x.toString().split(",");
			String txtName = row[0];
			String timestamp = row[1];		
			return new Tuple2<String, String>(txtName, timestamp);
	}};
	
	public static class string2PairRDD implements PairFunction<String, String, String>{
		/**
		 * Convert RDD<Row> to PairRDD<String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(String x)
				throws Exception {
			String [] row = x.toString().split(",");
			String txtName = row[0];
			String timestamp = row[1];		
			return new Tuple2<String, String>(txtName, timestamp);
	}};	
	
	public static class rawPath2TxtPath implements PairFunction<Tuple2<String, String>, String, String>{
		
		/**
		 * PairRDD<String, String> to PairRDD<String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(
				Tuple2<String, String> x) throws Exception {
			// base name
			String index = FilenameUtils.getBaseName(x._1()) +".txt";
			// content
			String value = x._2();
			return new Tuple2<String, String>(index, value);
		}
		
	};
	
	public static class FilterSmallSimilarities implements Function <Tuple2<String, Double>, Boolean> {
		/**
		 * PairRDD <String, Double> to PairRDD <String, Double>
		 */
		private static final long serialVersionUID = 1L;

		public Boolean call(Tuple2<String, Double> s) { if (s._2() > threshold) return true; return false; }
	};
    public static class GetDocParent implements Function2<String, String, String>{
	    /**
		 * GetHighestParent
		 * PairRDD <String, String> to PairRDD <String, String>
		 */
		private static final long serialVersionUID = 1L;

		public String call(String s1, String s2) {
			String [] s1_items = s1.split(",");
			String [] s2_items = s2.split(",");
			
			if (Double.parseDouble(s1_items[1]) > Double.parseDouble(s2_items[1]))
	        	return s1;
	        else
	        	return s2;
	    }
    };
  
    public static class DTWGetDocParent implements Function2<Tuple2<String, Double>, 
		Tuple2<String, Double>, Tuple2<String, Double>>{
	    /**
		 * GetHighestParent
		 * PairRDD <String, String> to PairRDD <String, String>
		 */
		private static final long serialVersionUID = 1L;
	
		public Tuple2<String, Double> call(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
			
			if (t1._2() < t2._2())
	        	return t1;
	        else
	        	return t2;
	    }
    };   

    
    public static class CleanJoins implements Function<Tuple2<String, Optional<String>>, Tuple2<String,String>>{
		/**
		 * PairRDD <String, Optional<String>> to PairRDD<String, String>
		 */
		private static final long serialVersionUID = 1L;

		  public Tuple2<String, String> call(Tuple2<String, Optional<String>> v1) throws Exception {
			  // base name
			  // tuple(timestamp, content)
			  String timestamp = (v1._2()+"").replace("Optional.of(", "").replace(")","");
			  return new Tuple2<String, String>(timestamp , v1._1());
		  }
		};
	
	public static class SwitchKey2Timestamp implements PairFunction
	<Tuple2<String, Tuple2<String,String>>, String, Tuple2<String,String>> {

		/**
		 * PairRDD <String, Tuple2<String,String>> to PairRDD <String, Tuple2<String,String>>
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Tuple2<String,String>> call(
				Tuple2<String, Tuple2<String,String>> x) throws Exception {
			// time stamp
			String index = x._2()._1();
			return new Tuple2<String, Tuple2<String,String>>
			(index, new Tuple2<String, String>( x._1(), x._2()._2()));
		}
		
	};
	
	public static class CleanKey2TxtName implements  PairFunction<Tuple2<String, Tuple2<String,String>>, String, String> {

		/**
		 * PairRDD <String, Tuple2<String,String>> to PairRDD <String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(
				Tuple2<String, Tuple2<String,String>> x) throws Exception {
			// base name
			String index = x._2()._1();
			// content
			String value = x._2()._2();
			return new Tuple2<String, String>(index, value);
		}
		
	};

	public static class GetValue implements Function<Tuple2<String, Double>, Double> {
	    /**
		 * PairRDD <String, String> to RDD <String>
		 */
		private static final long serialVersionUID = 1L;

		public Double call(Tuple2<String, Double> s) { return s._2(); }
	};
	
	public static class FormatNumber implements Function<Double, String> {
	    /**
		 * PairRDD <String, String> to RDD <String>
		 */
		private static final long serialVersionUID = 1L;

		public String call(Double s) { 
			DecimalFormat df = new DecimalFormat("#.#");
			return df.format(s); }
	};	
	
	
    public static class AddCountOne implements PairFunction<String, String, Integer> {
        /**
		 * PairRDD <String, Integer> to PairRDD <Integer, String>
		 */
		private static final long serialVersionUID = 1L;

        public Tuple2<String, Integer> call(String x) throws Exception {
            return new Tuple2(x, 1);
        }
    };	
	public static class GetTxtName implements Function<Tuple2<String, String>, String> {
	    /**
		 * PairRDD <String, String> to RDD <String>
		 */
		private static final long serialVersionUID = 1L;

		public String call(Tuple2<String, String> s) { return s._1(); }
	};

	
	public static class GetDocment implements Function<Tuple2<String, String>, List<String>> {
        /**
		 * Get the list of documents
		 * PairRDD <String, String> to RDD <List<String>>
		 */
		private static final long serialVersionUID = 1L;

        public List<String> call(Tuple2<String, String> fileNameContent) throws Exception {
            String content = fileNameContent._2();
            return Arrays.asList(content.replaceAll("\n", "").split(" "));
        }
    };
 
    
    public static class GetPredictedPairSimilarity implements PairFunction<String, String, Double>{

		/**
		 * RDD<String> to PairRDD <String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Double> call(String x)
				throws Exception {
			String [] items = x.replace(")", "").replace("MatrixEntry(", "").split(",");
			// key: doc,parent
			String index = items[0] + "," + items[1];
			// value: similarity score
			return new Tuple2<String, Double>(index, Double.parseDouble(items[2]));
	}};

	
	public static class GetDocPair implements PairFunction<Tuple2<String, Double>, String, String> {

		/**
		 * PairRDD <String, Double> to PairRDD <String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(
				Tuple2<String, Double> x) throws Exception {
			String [] items = x._1().split(",");
			// doc
			String index = items[0];
			// parent_doc
			//String value = items[1];						
			// parent_doc, similarity score
			String value = items[1] + "," + x._2();
			return new Tuple2<String, String>(index, value);
		}
		
	};
	 
    public static class AddUpOnes implements Function2<Integer, Integer, Integer>{
	    /**
		 * Add up ones
		 * PairRDD <String, Integer> to PairRDD <String, Integer>
		 */
		private static final long serialVersionUID = 1L;

		public Integer call(Integer i1, Integer i2) {
	        return i1 + i2;
	    }
    };
   
    
    public static class SortParentsBySimilarity implements 
    PairFunction<Tuple2<String, Iterable<String>>, String, String> {

		/**
		 * PairRDD <Integer, Iterable<String>> to PairRDD <String, String>
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(
				Tuple2<String, Iterable<String>> x) throws Exception {

			String index = x._1();
			String value = "";

			HashMap<Double, String> parent_similarity = new HashMap<Double, String>();
	        Iterator<String> iterator = x._2().iterator();
	        while (iterator.hasNext()) {
	        	String[] value_similarity = iterator.next().toString().split(",");
	        	// find the parent doc that has the highest similarity score
	        	double current_similarity = Double.parseDouble(value_similarity[1]);
	        	parent_similarity.put(current_similarity, value_similarity[0]);

	        }
	        Map<Double, String> sorted_parent_similarity = new TreeMap<Double, String>(parent_similarity); // ascending
	         Set<Entry<Double, String>> set2 = sorted_parent_similarity.entrySet();
	         Iterator<Entry<Double, String>> iterator2 = set2.iterator();
	         while(iterator2.hasNext()) {
	              Map.Entry<Double, String> me2 = (Map.Entry<Double, String>)iterator2.next();
	              value = me2.getValue()+" " + value; // descending
	         }
	         value = value.substring(0,value.length()-1);
			return new Tuple2<String, String>(index, value);
		}
		
	};
	
	public static class GetSortedDoc implements Function<Tuple2<String, Integer>, String> {
		/**
		 * PairRDD <String, Integer> to RDD<String>
		 */
		private static final long serialVersionUID = 1L;

		public String call(Tuple2<String, Integer> s) { return s._1(); }
	};	
}
