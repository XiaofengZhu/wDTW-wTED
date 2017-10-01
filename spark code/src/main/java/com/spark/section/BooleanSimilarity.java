package com.spark.section;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class BooleanSimilarity implements Serializable{
    protected double [] similarity_vector;
    protected List <DocumentVector> file_vector_list = new ArrayList<DocumentVector>();
    protected Vector<Integer> file_id_vector = new Vector<Integer>();
    protected HashMap<Integer, Double> file_pair_similarity = new HashMap<Integer, Double>();

    
    public void calMatrix(){
    	int file_count = file_vector_list.size();
    	this.similarity_vector = new double [file_count];
    	similarity_vector[0] = 0;
    		for (int j = 1; j < file_count; j++ ){
    			similarity_vector[j] = file_vector_list.get(0)
    					.getCosineSimilarityWith(file_vector_list.get(j)); 		
    			file_pair_similarity.put(
    					file_id_vector.get(j-1),
    					similarity_vector[j]);
    		}// end of for
    }// end of calMatrix

}