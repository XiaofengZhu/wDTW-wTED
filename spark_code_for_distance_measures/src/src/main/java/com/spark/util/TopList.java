package com.spark.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;

public class TopList {
	public TopList(){
		
	}
	public <K, V extends Comparable<? super V>> List<Entry<K, V>> 
	findGreatest(Map<K, V> map, int n){
	Comparator<? super Entry<K, V>> comparator = 
			new Comparator<Entry<K, V>>(){
			@Override
			public int compare(Entry<K, V> e0, Entry<K, V> e1){
				V v0 = e0.getValue();
				V v1 = e1.getValue();
				return v0.compareTo(v1);
			}
	};
	PriorityQueue<Entry<K, V>> highest = 
		new PriorityQueue<Entry<K,V>>(n, comparator);
	
	for (Entry<K, V> entry : map.entrySet()){
		highest.offer(entry);
		while (highest.size() > n){
			highest.poll();
		}
	}
	
	List<Entry<K, V>> result = new ArrayList<Map.Entry<K,V>>();
	while (highest.size() > 0){
		result.add(highest.poll());
	}
	Collections.reverse(result); 
	return result;
	}	

}
