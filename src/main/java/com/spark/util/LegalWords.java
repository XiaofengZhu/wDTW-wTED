package com.spark.util;

import java.io.IOException;

public class LegalWords {
	private StopWords stopWords;		
	private Words Words;
	
	public LegalWords() throws IOException{
		stopWords = new StopWords();		
		Words = new Words();
	}
	
	public boolean isLegalWord(String word){
    	if (Words.isWord(word)){
        	if (!stopWords.isStopWord(word)){
        		return true;
        	}// end of if        		
    	}// end of if
    	return false;
	}
}
