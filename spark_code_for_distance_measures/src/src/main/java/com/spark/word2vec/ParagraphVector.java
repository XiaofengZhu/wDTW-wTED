package com.spark.word2vec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;

public class ParagraphVector implements Serializable{
	
	Word2VecModel model;
	public ParagraphVector(SparkContext sc, Word2VecModel model){
	    JavaSparkContext jsc = new JavaSparkContext(sc);
	    
		this.model = model;		
	}
	
	public Vector getWordVector(Word2VecModel model, String word){
		try{
			return model.transform(word);
			}catch(Exception e){
				return null;
			}
	}
	
	/*
	public List<Vector> getParagraphVector(Word2VecModel model, String paragraph_string){
		List<Vector> paragraphVector = new ArrayList<Vector>();
		for(String word: paragraph_string.split(" ")){
			paragraphVector.add(this.getWordVector(model, word));
		}
		return paragraphVector;
	}
	*/
	
	public String replaceWordWithVector(String document_string){
		StringBuilder vectorStringBuilder = new StringBuilder();
		String spaces = "[\\s\\r\\t]+";
		Pattern pattern = Pattern.compile(spaces);
		String [] paras = document_string.split("\n");
		for (int i = 0; i < paras.length; i++){
			String para = paras[i];
			Matcher matcher = pattern.matcher(para.trim());
			para = matcher.replaceAll(" ");
			if (para.length() >= 1) {
				String[] words = para.split(" ");
				for(int j = 0; j < words.length; j++){
					String word = words[j];
					Vector vec = this.getWordVector(this.model, word);
					if (vec != null){
						if (j < words.length - 1) {
							vectorStringBuilder.append(vec +" ");
						} else {
							vectorStringBuilder.append(vec);
						}	
					}
				}
				if (i < paras.length - 1) {
					// paragraphs are separated by \t
					vectorStringBuilder.append("\t");
				}				
			}
		}
		
		return vectorStringBuilder.toString();
	}
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
	    // Note that we run with local[2], 
	    // meaning two threads, 
	    // which can help detect bugs that only exist when we run in a distributed context.
	    // in reality, the number of clusters also depends on the data set
	    .setMaster("local[*]")
	    .setAppName("Paragraph Vector");
	    
	    SparkContext sc = new SparkContext(conf);
	    JavaSparkContext jsc = new JavaSparkContext(sc);
	    
		//Word2VecModel model = Word2VecModel.load(sc, "word2vec.model");

	}	

}
