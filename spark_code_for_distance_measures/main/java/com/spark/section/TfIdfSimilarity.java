package com.spark.section;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.spark.util.TopList;


public class TfIdfSimilarity extends BooleanSimilarity implements Serializable{

	private List <TfIdfDocumentVector> tfidf_file_vector_list = new ArrayList<TfIdfDocumentVector>();
    
    public void readList(List<String> sections) throws Exception{
    	
        WordVector wordVector = new WordVector();
    	TfIdfDocumentVector dv;

    	int row = 0;
    	for (String section : sections){
    		dv = new TfIdfDocumentVector();
    		String [] words = section.split(" ");
            for(String w : words) {
                dv.incCount(w);
                wordVector.incIdfCount(w, row);
            }// end of for
            file_id_vector.add(row);
            dv.tfCount(words.length);
            if (dv!= null){
            	file_vector_list.add(dv);
        	}
            row++;
    	}// end of for  
    	
        wordVector.updateIdfCount(file_id_vector.size());
        Map<String, Double> idfWordMap = wordVector.getIdfWordMap();
        if (idfWordMap.size() > 0){
        for (int i = 0; i < file_vector_list.size(); i++){
	        	dv = (TfIdfDocumentVector) file_vector_list.get(i);	        	
	        	
//	        	if (dv==null){
//	        		System.out.println("dv.tfIdfCount(idfWordMap): "+ (file_vector_list.get(i)==null) + " "+ (idfWordMap==null));
//	        		}
	        	if (dv!=null){
	        		dv.tfIdfCount(idfWordMap);
	        		tfidf_file_vector_list.add(dv );
	        	}
        	}
        } 	
    }// end of readlists    

    public void calMatrix(){
    	try{
	    	if (tfidf_file_vector_list != null){
		    	int file_count = tfidf_file_vector_list.size();
		    	this.similarity_vector = new double [file_count];
		    	this.file_pair_similarity = new HashMap<Integer, Double>();
		    	if (file_count >0)
		    		similarity_vector[0] = 0;
				for (int j = 1; j < file_count; j++ ){
					similarity_vector[j]  = tfidf_file_vector_list.get(0)
							.getCosineSimilarityWith(tfidf_file_vector_list.get(j)); 	
					file_pair_similarity.put(
							file_id_vector.get(j-1),
							similarity_vector[j]);
				}// end of for
			}
    	}catch(Exception e){}

    }// end of calMatrix
      
    
    public Entry<Integer, Double> getTopSimilarities() throws IOException{
    	calMatrix();
    	try{
			TopList topList = new TopList();
	        List<Entry<Integer, Double>> resultList = topList.findGreatest(this.file_pair_similarity, 1);
	        if (resultList.size() > 0)
	        	return resultList.get(0);  
	        else return null;
        }catch(Exception e) {return null;}
    }
}
