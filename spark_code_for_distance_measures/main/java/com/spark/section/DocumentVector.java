package com.spark.section;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DocumentVector implements Serializable{
    Map<String, Double> wordMap = new HashMap<String, Double>();

    public Map<String, Double> getWordMap() {
		return wordMap;
	}

	public void incCount(String word) {
        Double oldCount = wordMap.get(word);
        wordMap.put(word, oldCount == null ? 1 : oldCount + 1);
    }

    double getCosineSimilarityWith(DocumentVector otherVector) {
        double innerProduct = 0;
        for(String w: this.wordMap.keySet()) {
            innerProduct += this.getCount(w) * otherVector.getCount(w);
        }
        if (this.getNorm() == otherVector.getNorm()  && this.getNorm()== 0.0) return 1.0;
        if (innerProduct == 0) return 0.0;
        return innerProduct / (this.getNorm() * otherVector.getNorm());
    }

    double getNorm() {
        double sum = 0;
        for (Double count : wordMap.values()) {
            sum += count * count;
        }
        return Math.sqrt(sum);
    }

    double getCount(String word) {
        return wordMap.containsKey(word) ? wordMap.get(word) : 0;
    }  
    
    public static void main(String[] args) {
// example
//        String doc1 = "Today is a good day.";
//        String doc2 = "Today is a good day, A.";
//
//        DocumentVector v1 = new DocumentVector();
//        for(String w:doc1.split("[^a-zA-Z]+")) {
//            v1.incCount(w);
//        }
//
//        DocumentVector v2 = new DocumentVector();
//        for(String w:doc2.split("[^a-zA-Z]+")) {
//            v2.incCount(w);
//        }
//
//        System.out.println("Similarity = " + v1.getCosineSimilarityWith(v2));
    }

}