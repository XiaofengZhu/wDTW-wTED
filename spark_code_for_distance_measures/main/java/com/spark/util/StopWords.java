package com.spark.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class StopWords {
	private HashSet <String> hashSet = new HashSet <String>();
	private static final String stopWords_file_name = "stopwords.txt";
	
	public StopWords() throws IOException{
		File file = new File(stopWords_file_name);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			this.hashSet.add(line);
		}
		fileReader.close();		
	}
	
	public boolean isStopWord(String word){
		if (this.hashSet.contains(word.toLowerCase())) return true;
		else return false;
	}
}
