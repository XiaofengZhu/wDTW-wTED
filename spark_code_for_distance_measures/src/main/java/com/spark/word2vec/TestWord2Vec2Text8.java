package com.spark.word2vec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import com.google.common.base.Strings;


public class TestWord2Vec2Text8 {
	private static JavaSparkContext sc;
	private static SparkConf conf;
	public static void main(String[] args) {
	    conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads, 
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setMaster("local[*]")
	    .setAppName("Doc2Vec Test");
	    
	    sc = new JavaSparkContext(conf);
	    
	    // The tests are to check Java compatibility.
	    String sentence = Strings.repeat("a b ", 100) + Strings.repeat("a c ", 10);
	    List<String> words = Arrays.asList(sentence.split(" "));
	    List<List<String>> localDoc = Arrays.asList(words, words);
	    JavaRDD<List<String>> doc = sc.parallelize(localDoc);

	    Word2Vec word2vec = new Word2Vec()
	      .setVectorSize(10)
	      .setSeed(42L);
	    Word2VecModel model = word2vec.fit(doc);
	    Tuple2<String, Object>[] syms = model.findSynonyms("a", 2);
	    System.out.println("syms[0]._1(): "+syms[0]._1() +"+ syms[0]._2(): "+syms[0]._2());
	    System.out.println("syms[1]._1(): "+syms[1]._1() +"+ syms[1]._2(): "+syms[1]._2());

	}

}
