package com.spark.local;

/* CosineSimilarity.java */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

public class SetParameters {
    /**
	 * read and set up parameters 
	 * for document revision calculation
	 * 
	 */
	

	private String master, databaseName,username, password, tableName, sectionRelationTableName,
	objectFieldFromSectionRelationTable,txtFileNameFieldFromFileTable, timestampFieldFromFileTable,	
	rootPath, corpusFolder, inputPath, csvFilePath, fileNameFolder, matrixFolder, dtwFolder, 
	wmdFolder, outputFolder, word2vecFolder, outputSectionFolder;	

	
	// constructor
	SetParameters(){
		initilizeParameters();	
		checkOutputFolders();
	}

	public void checkOutputFolders(){
	    File outputFile = new File(this.outputFolder);	
	    File outputSectionFile = new File(this.outputSectionFolder);	
		File fileNameFile = new File(this.fileNameFolder);
		File matrixFile = new File(this.matrixFolder);
		File dtwFile = new File(this.dtwFolder);
		File wmdFile = new File(this.wmdFolder);
		if (fileNameFile.exists())
			try {
				FileUtils.deleteDirectory(fileNameFile);
			} catch (IOException e) {
				System.err.println("fileNames folder exists. Fail to delete this folder");
			}	
		if (dtwFile.exists())
			try {
				FileUtils.deleteDirectory(dtwFile);
			} catch (IOException e) {
				System.err.println("dtwFolder folder exists. Fail to delete this folder");
			}	
		if (wmdFile.exists())
			try {
				FileUtils.deleteDirectory(wmdFile);
			} catch (IOException e) {
				System.err.println("WMDFolder folder exists. Fail to delete this folder");
			}			
		if (matrixFile.exists())
			try {
				FileUtils.deleteDirectory(matrixFile);
			} catch (IOException e) {
				System.err.println("matrix folder exists. Fail to delete this folder");
			}
		if (outputFile.exists())
			try {
				FileUtils.deleteDirectory(outputFile);
			} catch (IOException e) {
				System.err.println("output folder exists. Fail to delete this folder");
			}	
		if (outputSectionFile.exists())
			try {
				FileUtils.deleteDirectory(outputSectionFile);
			} catch (IOException e) {
				System.err.println("output_section folder exists. Fail to delete this folder");
			}			
	}

	// set parameters
	private void setCorpusDir(Properties prop) {
        if (prop.getProperty("rootPath")!=null){
        	//--read corpus dir
        	
        	this.rootPath = prop.getProperty("rootPath").trim(); 
        	this.corpusFolder = prop.getProperty("corpusFolder").trim(); 
        	this.inputPath = this.rootPath + this.corpusFolder;
        	/*
        	if(!new File(this.inputPath).exists()){
        		System.err.println("Input corpus dir is incorrect");
        		System.exit(0);	
    		}
        	*/
        	this.fileNameFolder = this.rootPath + "fileNames_" + this.corpusFolder;	
    		this.matrixFolder = this.rootPath + "intermediateResults_" + this.corpusFolder;
    		this.dtwFolder = this.rootPath + "dtwFolder_" + this.corpusFolder;	
    		this.wmdFolder = this.rootPath + "wmdFolder_" + this.corpusFolder;	
    		this.word2vecFolder= this.rootPath + "word2vecFolder_" + this.corpusFolder;	
    		//this.outputFolder = this.rootPath + "outFolder_" + this.corpusFolder;	
		}else{
			System.err.println("corpus info is required!");
		    System.exit(0); 			
		}            
        
	}// end of setCorpusDir(...)

	private void setCsv(Properties prop) {
	    if (prop.getProperty("csvFilePath")!=null)
	    	//--set relationTableName
	    	this.csvFilePath = prop.getProperty("csvFilePath").trim();   
	    
	}// end of setsetCsv(...)
	
	private void setDatabase(Properties prop) {
        if (prop.getProperty("databaseName")!=null 
        		&& prop.getProperty("databaseUserName")!=null
        		&& prop.getProperty("databasePassword")!=null){
        	//--read databaseName
        	this.databaseName = prop.getProperty("databaseName").trim();
        	//--read database username
        	this.username = prop.getProperty("databaseUserName").trim();
        	//--read database password
        	this.password = prop.getProperty("databasePassword").trim();                  	
        }else{                	
			System.err.println("MySQL DB info is required!");
		    System.exit(0);    			    
        } 
	}// end of setDatabase(...)
 
	private void setMaster(Properties prop) {
	    if (prop.getProperty("master")!=null)
	    	//--set relationTableName
	    	this.master = prop.getProperty("master").trim();   
	    else{                	
			System.err.println("spark mode info is required!");
		    System.exit(0);    			    
        }	    
	}// end of setMaster(...)

	private void setFileTableName(Properties prop) {
	    if (prop.getProperty("fileTableName")!=null)
	    	//--set fileTableName(...)
	    	this.tableName = prop.getProperty("fileTableName").trim(); 
	    else{                	
			System.err.println("fileTableName info is required!");
		    System.exit(0);    			    
        }	    
	}// end of setFileTableName(...)
	private void setSectionRelationTableName(Properties prop) {
	    if (prop.getProperty("sectionRelationTableName")!=null)
	    	this.sectionRelationTableName = 
	    	prop.getProperty("sectionRelationTableName").trim();  
	    else{                	
			System.err.println("sectionRelationTableName info is required!");
		    System.exit(0);    			    
        } 	    
	}// end of setFileTableName(...)	
	private void setOutputFolder(Properties prop){
	    if (prop.getProperty("outputFolder")!=null)
	    	this.outputFolder = this.rootPath + prop.getProperty("outputFolder").trim();  		
	    else{                	
			System.err.println("outputFolder info is required!");
		    System.exit(0);    			    
        } 		
	}// end of setOutputFolder(...)
	private void setOutputSectionFolder(Properties prop){
	    if (prop.getProperty("outputSectionFolder")!=null)
	    	this.outputSectionFolder = prop.getProperty("outputSectionFolder").trim();  		
	    else{                	
			System.err.println("outputSectionFolder info is required!");
		    System.exit(0);    			    
        } 				
	}// end of setOutputSectionFolder(...)
	
	public void setObjectFieldFromSectionRelationTable(Properties prop) {
		if (prop.getProperty("objectFieldFromSectionRelationTable")!=null)
			this.objectFieldFromSectionRelationTable = 
				prop.getProperty("objectFieldFromSectionRelationTable").trim();
	    else{                	
			System.err.println("objectFieldFromSectionRelationTable info is required!");
		    System.exit(0);    			    
        } 		
	}// end of setObjectFieldFromSectionRelationTable

	public void setTxtFileNameFieldFromFileTable(Properties prop) {
		if (prop.getProperty("txtFileNameFieldFromFileTable")!=null)
			this.txtFileNameFieldFromFileTable = 
				prop.getProperty("txtFileNameFieldFromFileTable").trim();
	    else{                	
			System.err.println("txtFileNameFieldFromFileTable info is required!");
		    System.exit(0);    			    
        } 		
	}// end of setTxtFileNameFieldFromFileTable

	public void setTimestampFieldFromFileTable(Properties prop) {
		if (prop.getProperty("timestampFieldFromFileTable")!=null)
			this.timestampFieldFromFileTable = 
				prop.getProperty("timestampFieldFromFileTable").trim();
	    else{                	
			System.err.println("timestampFieldFromFileTable info is required!");
		    System.exit(0);    			    
        }		
	}// end of 	setTimestampFieldFromFileTable
	
	private void initilizeParameters(){
		Properties prop = new Properties();
		try{
			
			//get project current path
            String path = System.getProperty("user.dir"); 
            // read settings from configuration file -- parameter.properties
            String propertiesDir =path + File.separator + "parameter.properties";
            
            if (new File(propertiesDir).exists()){
            	
            	//load properties file
                prop.load(new FileInputStream(propertiesDir));	
                
                /** corpusDir, databaseName, databaseUsername, databasePassword, 
                 * master, fileTableName and outputFolder are required fields*/                   
                setCorpusDir(prop);
                setCsv(prop);
                setDatabase(prop);
                setMaster(prop);
                setFileTableName(prop);
                setOutputFolder(prop);
                setSectionRelationTableName(prop);
                setOutputSectionFolder(prop);
                setObjectFieldFromSectionRelationTable(prop);
                setTxtFileNameFieldFromFileTable(prop);
                setTimestampFieldFromFileTable(prop);

            }else{
            	System.err.println("file \"parameter.properties\" not found");        		     
			}// end of loading properties file 
            
        }catch(FileNotFoundException fnfe){
            System.err.println("file \"parameter.properties\" not found");
            fnfe.printStackTrace();
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
        }		
	}// end of initilizeParameters	

	
	public String getMaster() {
		return master;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getUsername() {
		return username;
	}
	
	public String getPassword() {
		return password;
	}
	public String getTableName() {
		return tableName;
	}
	public String getObjectFieldFromSectionRelationTable() {
		return objectFieldFromSectionRelationTable;
	}	
	public String getTxtFileNameFieldFromFileTable() {
		return txtFileNameFieldFromFileTable;
	}	
	public String getTimestampFieldFromFileTable() {
		return timestampFieldFromFileTable;
	}	
	public String getSectionRelationTableName() {
		return sectionRelationTableName;
	}	
	
	
	public String getRootPath() {
		return this.rootPath;
	}	
	public String getCorpusFolder(){
		return this.corpusFolder;
	}	
	public String getInputPath() {
		return this.inputPath;
	}
	public String getCsvFilePath() {
		return csvFilePath;
	}		
	public String getFileNameFolder() {
		return this.fileNameFolder;
	}
	public String getDtwFolder() {
		return dtwFolder;
	}	
	public String getWmdFolder() {
		return wmdFolder;
	}
	public String getMatrixFolder() {
		return matrixFolder;
	}	
	public String getOutputFolder() {
		return outputFolder;
	}	
	public String getWord2vecFolder(){
		return word2vecFolder;
	}
	
}
