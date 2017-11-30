package com.spark.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Words {
	private static final String regex_string = "[a-zA-Z][a-zA-Z'-]+";
	private Pattern pattern;
	private Matcher matcher;	
	
	public Words(){
		pattern = Pattern.compile(regex_string);
	}
	
	public boolean isWord(String word){
		word = word.replaceAll(" ", "");
		this.matcher = pattern.matcher(word);
		if (this.matcher.find()) return true;
		else return false;
	}
}
