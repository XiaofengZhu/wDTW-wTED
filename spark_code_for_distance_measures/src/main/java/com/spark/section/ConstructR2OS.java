package com.spark.section;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class ConstructR2OS implements Serializable{
	private static double threshold = 0.5;// default section similarity tao value
	private TfIdfSimilarity tfIdfSimilarity;
	
	public ConstructR2OS(){
	}
	
	public List<String> construct(List<String> rs_section_strings, List<String> os_section_strings) throws Exception{
		List<String> simialr_section = new ArrayList<String>();
		for (String r: rs_section_strings){
			List<String> ros_section_strings = new ArrayList<String>();
			tfIdfSimilarity = new TfIdfSimilarity();

			ros_section_strings.add(r);
			for (String o: os_section_strings){
				ros_section_strings.add(o);
			}
			tfIdfSimilarity.readList(ros_section_strings);
			// id, similarity value
			Entry<Integer, Double> entry = tfIdfSimilarity.getTopSimilarities();
			simialr_section.add(entry.getValue()+"");
			/*
			if (entry.getValue()> threshold){	
				simialr_section.add(entry.getValue()+"");
				simialr_section.add("REVISION: "+r +"\nORIGIN: "+ os_section_strings.get(entry.getKey())
						+ "\nSimilarity: "+entry.getValue());
			}*/

		}

		return simialr_section;
	}
	
	public Double construct(String [] revised_strings, String []  old_strings) throws Exception{
		double revisedDoc_odlDoc_distance = 0; // the smaller the better
		int counts = 0;
		for (String r: revised_strings){
			List<String> ros_section_strings = new ArrayList<String>();
			tfIdfSimilarity = new TfIdfSimilarity();
			if (r.length() > 1)
				ros_section_strings.add(r);
			for (String o: old_strings){
				if (o.length() > 1)
					ros_section_strings.add(o);
			}
			if (ros_section_strings.size() > 0 && ros_section_strings.size() > 0){
				try{
					tfIdfSimilarity.readList(ros_section_strings);
					// id, similarity value
					Entry<Integer, Double> entry = tfIdfSimilarity.getTopSimilarities();
					if (entry != null){
						//System.out.println("entry.getValue(): " + entry.getValue());
						revisedDoc_odlDoc_distance += entry.getValue();
						counts ++;
						}
				}catch(Exception e){
					System.out.println("Exception !!!!");
					return Double.POSITIVE_INFINITY;
				}			
			}


		}

		return revisedDoc_odlDoc_distance/counts;
	}	
	
	public static void main(String[] args) throws Exception {
//		List<String> rs_section_strings = new ArrayList<String>();
//		List<String> os_section_strings = new ArrayList<String>();
//		rs_section_strings.add("Inductance is either due to spheres or planes");
//		rs_section_strings.add("Chapter two");
//		os_section_strings.add("Inductance is either due to spheres or planes");
//		os_section_strings.add("Chapter two");	
//		
//		ConstructR2OS constructR2OS = new ConstructR2OS();
//		List<String> result_list = constructR2OS.construct(rs_section_strings, os_section_strings);
//		for (String strs: result_list){
//			System.out.println(strs);
//		}		
		String revised_doc = "Inductance is either due to spheres or planes \n Chapter two";
		String old_doc = "Inductance is either due to spheres or planes \n Chapter two";
		
		ConstructR2OS constructR2OS = new ConstructR2OS();
		System.out.println(constructR2OS.construct(revised_doc.split("\n"), old_doc.split("\n")));

	}

}
