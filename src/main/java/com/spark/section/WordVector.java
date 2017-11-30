package com.spark.section;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WordVector implements Serializable{
    private Map<String, Double> idfWordMap = new HashMap <String, Double>();
    private Set<String> unique_word_file = new HashSet<String>();
   
    public Map<String, Double> getIdfWordMap() {
		return idfWordMap;
	}

	public void incIdfCount(String word, int row) {
		if (!unique_word_file.contains(word+row)){
			unique_word_file.add(word+row);
	        Double oldCount = idfWordMap.get(word);
	        idfWordMap.put(word, oldCount == null ? 1 : oldCount + 1);
        }
    }
 
    public void updateIdfCount(int N) {
        for (String word: idfWordMap.keySet())
        	this.idfWordMap.put(word, Math.log((double)N/idfWordMap.get(word)));
    }     
}
