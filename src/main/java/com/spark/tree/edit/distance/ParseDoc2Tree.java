package com.spark.tree.edit.distance;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets; 
import java.nio.file.Files; 
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseDoc2Tree implements Serializable{

	public int getHashCode(String paragraph_string){
		return paragraph_string.hashCode();
	}
	
	public String getDocumentTree(String file_name, boolean file_path) throws IOException{
		
		String document_string = new String(Files.readAllBytes(Paths.get(file_name)), StandardCharsets.UTF_8); 

		String [] paras = document_string.replaceAll("[\r\n]+", "\n").split("\n");
		String root = paras[0];
		
		StringBuilder document_tree_code_StringBuilder= new StringBuilder();
//		document_tree_code_StringBuilder.append("{" + this.getHashCode(root));
		document_tree_code_StringBuilder.append("{" + root);
		
		
		for (int i = 1; i < paras.length-1; i++){
			String paragraph_string = paras[i];
			if (paragraph_string.length() > 2)
				document_tree_code_StringBuilder.append("{" + paragraph_string + "}");
//				document_tree_code_StringBuilder.append("{" + this.getHashCode(paragraph_string) + "}");
		}
		
		document_tree_code_StringBuilder.append("}");
		return document_tree_code_StringBuilder.toString();
	}// end of getDocumentTree

	public String getDocumentTree(String root, String document_string){

		String [] paras = document_string.replaceAll("[\r\n]+", "\n")
				.replaceAll("[^\\S\\r\\n]+", " ").split("\n");

		if (root.length() == 0) root = "NULL";
		
		StringBuilder document_tree_code_StringBuilder= new StringBuilder();
		document_tree_code_StringBuilder.append("{" + root);
		
		
		for (int i = 0; i < paras.length-1; i++){
			String paragraph_string = paras[i];
			if (paragraph_string.length() > 2)
				document_tree_code_StringBuilder.append("{" + paragraph_string + "}");
		}
		
		document_tree_code_StringBuilder.append("}");
		return document_tree_code_StringBuilder.toString();
	}// end of getDocumentTree
	
	
	public ArrayList<String> getParas(String document_string){
		String spaces = "[\\s\\r]+";
		Pattern pattern = Pattern.compile(spaces);
		ArrayList<String> paraList = new ArrayList<String>();
		for (String str : document_string.split("\n")) {// \\*\\*\\*
			Matcher matcher = pattern.matcher(str.trim());
			str = matcher.replaceAll(" ");
			if (str.length() >= 1) paraList.add(str);
		}

		return paraList;
	}
	
	public String getDocumentTree(String document_string){

		ArrayList<String> paras = this.getParas(document_string);
		String root;
		
		if (paras.size() == 0) root = "NULL";
		else root = paras.get(0);
		
		StringBuilder document_tree_code_StringBuilder= new StringBuilder();
		document_tree_code_StringBuilder.append("{" + root);
		
		
		for (int i = 1; i < paras.size()-1; i++){
			String paragraph_string = paras.get(i);
			if (paragraph_string.length() >= 1)
				document_tree_code_StringBuilder.append("{" + paragraph_string + "}");
		}
		
		document_tree_code_StringBuilder.append("}");
		return document_tree_code_StringBuilder.toString();
	}// end of getDocumentTree
		
	public static void main (String [] args) throws IOException{
		ParseDoc2Tree parseDoc2Tree = new ParseDoc2Tree();
		String file_name = "C:/Users/Xiaofeng/Dropbox/run6/corpus1/1_1_1_txt-Sulfite-3_1.0-2_2.0-5_3.0.txt";
		System.out.println(parseDoc2Tree.getDocumentTree(file_name, true));
	}
}
