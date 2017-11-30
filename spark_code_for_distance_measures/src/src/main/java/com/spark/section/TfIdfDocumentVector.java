package com.spark.section;

import java.io.Serializable;
import java.util.Map;

public class TfIdfDocumentVector extends DocumentVector implements Serializable{

    public void tfCount(int size) {
        for (String word: wordMap.keySet())
        	this.wordMap.put(word, 1 + Math.log((double)wordMap.get(word)/size));
    }    
    
    public void tfIdfCount(Map<String, Double> idfWordMap) {
    	if (idfWordMap.size() > 0 && wordMap.size() > 0)
        for (String word: wordMap.keySet())
        	try{
        		if (idfWordMap.containsKey(word) && wordMap.containsKey(word))
        		this.wordMap.put(word, idfWordMap.get(word) * wordMap.get(word));
        		//this.wordMap.put(word, 1 * idfWordMap.get(word));
        	}catch(Exception e){}
    } 

}