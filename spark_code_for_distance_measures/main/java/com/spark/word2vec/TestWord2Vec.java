package com.spark.word2vec;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;

public class TestWord2Vec {
	private static SparkContext sc;
	private static JavaSparkContext jsc;
	private static SparkConf conf;
	
	public static void main(String[] args) {
	    conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads, 
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    //.setMaster("local[*]")
	    .setAppName("Doc2Vec Test");
	    
//	    jsc = new JavaSparkContext(conf);
	    sc = new SparkContext(conf);
	    jsc = new JavaSparkContext(sc);
	    
//	    JavaRDD<List<String>> txtNames_rowRdd = jsc.textFile("C:/Users/Xiaofeng/Dropbox/run6/corpus6")
//	    		.map(line -> Arrays.asList(line.split(" ")));
//	    Word2Vec word2vec = new Word2Vec()
//	      .setVectorSize(10)
//	      .setSeed(42L);
//	    Word2VecModel model = word2vec.fit(txtNames_rowRdd);
//	    // Save and load model
//	    scala.collection.immutable.Map<String,float[]> wordVectors = model.getVectors();

//	    
//	    model.save(sc, "word2vec.model");
	    
	    //model = Word2VecModel.load(sc, "word2vec.model");

	}

}
