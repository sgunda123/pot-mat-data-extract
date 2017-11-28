package com.reltio.extract.denormalized.service;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang3.ArrayUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.reltio.cst.service.ReltioAPIService;
import com.reltio.cst.service.TokenGeneratorService;
import com.reltio.cst.service.impl.SimpleReltioAPIServiceImpl;
import com.reltio.cst.service.impl.TokenGeneratorServiceImpl;
import com.reltio.extract.domain.Attribute;
import com.reltio.extract.domain.BigMatch;
import com.reltio.extract.domain.Configuration;
import com.reltio.extract.domain.EntityTypes;
import com.reltio.extract.domain.ExtractConstants;
import com.reltio.extract.domain.ExtractProperties;
import com.reltio.extract.domain.HObject;
import com.reltio.extract.domain.InputAttribute;
import com.reltio.extract.domain.ReltioObject;
import com.reltio.extract.domain.ScanResponse;
import com.reltio.extract.util.ExtractServiceUtil;
import com.reltio.file.ReltioCSVFileWriter;
import com.reltio.file.ReltioFileWriter;
import com.reltio.file.ReltioFlatFileWriter;



/**
 * 
 * This is the main class for generating the Extract Report
 * 
 * @author Mohan
 * 
 */
public class AttributeExtractReportForPotenMatchesV4 {

		private static Gson GSON = new Gson();
		private static final String[] DefaultAttributes = { "ReltioURI"};
		private static DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		private static Map<Integer,String> matchUri = null;
		private static Map<Integer,String> matchLabel = null;
		public static void main(String[] args) throws Exception {
			
			System.out.println("Extract Process Started..");
			long programStartTime = System.currentTimeMillis();
			Properties properties = new Properties();
			
			try {
				String propertyFilePath = args[0]; 
				FileReader in = new FileReader(propertyFilePath);
//				InputStream in = new AttributeExtractReportForPotenMatchesV4().getClass().getResourceAsStream("/dev-ov.properties");
				properties.load(in);
			} catch (Exception e) {
				System.out.println("Failed reading properties File...");
				e.printStackTrace();
			}
			
			// READ the Properties values
			final ExtractProperties extractProperties = new ExtractProperties(
					properties);

			// VERIFY the required properties
			if (extractProperties.getApiUrl() == null
					|| extractProperties.getEntityType() == null
					|| extractProperties.getOutputFilePath() == null
					|| extractProperties.getUsername() == null
					|| extractProperties.getPassword() == null
					|| extractProperties.getTransitive_match() == null
					|| extractProperties.getFileFormat() == null
					|| extractProperties.getAuthUrl() == null
					|| extractProperties.getThreadCount() == null) {
				System.out
						.println("Error::: one or more required parameters missing. Please verify the input properties File....");
				System.exit(0);
			}

			final String matchType=extractProperties.getTransitive_match();
			TokenGeneratorService tokenGeneratorService = new TokenGeneratorServiceImpl(
					extractProperties.getUsername(),
					extractProperties.getPassword(), extractProperties.getAuthUrl());
			final ReltioAPIService reltioAPIService = new SimpleReltioAPIServiceImpl(
					tokenGeneratorService);
			
			
			String configRes = reltioAPIService.get(extractProperties.getApiUrl()+"/configuration/_noInheritance");
			Configuration configObj = GSON.fromJson(configRes,Configuration.class);
			matchUri = new LinkedHashMap<Integer,String>();
			matchLabel= new LinkedHashMap<Integer,String>();
			
			for (EntityTypes entT : configObj.getEntityTypes()) {
				
				if (entT.getUri().equals("configuration/entityTypes/"+extractProperties.getEntityType())) {
					
					int i=0;
					if (entT.getMatchGroups() !=null && !entT.getMatchGroups().isEmpty())
					for (Attribute attr : entT.getMatchGroups() ) {
						i++;
						 matchUri.put(Integer.valueOf(i), attr.getUri().trim());
				            matchLabel.put(Integer.valueOf(i), attr.getLabel().trim());
					}
				}
			}
			
			
			int threadsNumber = extractProperties.getThreadCount();	
			Integer count = 0;
			Integer processedCount = 0;
			
			final Map<String, InputAttribute> attributes = new LinkedHashMap<>();		

			// Read OV Values Attribute
			if (extractProperties.getOvAttrFilePath() != null
					&& !extractProperties.getOvAttrFilePath().isEmpty()
					&& !extractProperties.getOvAttrFilePath().equalsIgnoreCase(
							"null")) {

				BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(extractProperties.getOvAttrFilePath()), "UTF-8"));
				ExtractServiceUtil.createAttributeMapFromProperties(reader,
						attributes);
			}
			
			final List<String> fileResponseHeader = new ArrayList<String>();

			for (String header : DefaultAttributes) {
				fileResponseHeader.add(header);
			}

			// Create Response file header
			for (Map.Entry<String, InputAttribute> attr : attributes.entrySet()) {
				ExtractServiceUtil.createExtractNestedResponseHeader(attr,fileResponseHeader, null);
			}
			
			final ReltioFileWriter reltioFile;

			// Output File
			// check whether its CSV out FILE or Flat File
			if (extractProperties.getFileFormat().equalsIgnoreCase("CSV")) {
				reltioFile = new ReltioCSVFileWriter(
						extractProperties.getOutputFilePath());
			} else if (extractProperties.getFileDelimiter() != null) {
				// provided file Delimiter
				reltioFile = new ReltioFlatFileWriter(
						extractProperties.getOutputFilePath(),
						ExtractConstants.ENCODING,
						extractProperties.getFileDelimiter());
			} else {

				// Default delimiter pipe
				reltioFile = new ReltioFlatFileWriter(
						extractProperties.getOutputFilePath());
			}

			final String[] responseHeader = new String[fileResponseHeader.size()];
			fileResponseHeader.toArray(responseHeader);
			
			
			String[] matRuleHead= new String[1];
			matRuleHead[0]="Rule";			
			String[] finHeaderArray =(String[])ArrayUtils.addAll(matRuleHead, concatHeaderArray (responseHeader,responseHeader));
			

			if (extractProperties.getIsHeaderRequired() == null
					|| extractProperties.getIsHeaderRequired().equalsIgnoreCase(
							"Yes")) {
				reltioFile.writeToFile(finHeaderArray);
			}
			final String apiUrl =  extractProperties.getApiUrl(); 
			String filterUrl = "filter=equals(type,'configuration/entityTypes/"+extractProperties.getEntityType() +"') and range(matches,1,3000)";
			final String scanUrl = apiUrl + "/entities/_scan?"+filterUrl+"&select=uri&max=100";
//			final String scanUrl = apiUrl + "/entities/_scan?filter=equals(uri,'entities/YbhrbGy')&select=uri";
			String incReportURLTotal = apiUrl + "/entities/_total?"+filterUrl;
			System.out.println("Total=="+reltioAPIService.get(incReportURLTotal));  
			System.out.println(scanUrl);
			String intitialJSON = "";
			
			ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadsNumber);
			boolean eof = false;
			ArrayList<Future<Long>> futures = new ArrayList<Future<Long>>();

		while (!eof) {
			
				// Doing the DBScan API Call here
				String scanResponse = reltioAPIService.post(scanUrl,intitialJSON);

				// Convert the string the java object
				ScanResponse scanResponseObj = GSON.fromJson(scanResponse,ScanResponse.class);

				if (scanResponseObj.getObjects() != null&& scanResponseObj.getObjects().size() > 0) {						
					
					count += scanResponseObj.getObjects().size();
					System.out.println("Scaned records count = " + count);
										
					final List<ReltioObject> objectsToProcess = scanResponseObj.getObjects();	
					
					int threadNum = 0;
								
					for (final ReltioObject objectsToProces : objectsToProcess) {	
						
					threadNum++;
				
					futures.add(executorService.submit(new Callable<Long>() {
						@Override
						public Long call() throws Exception {
							long requestExecutionTime = 0l;
							try {
								String getResponse ="";
								if(matchType==null||matchType.equalsIgnoreCase("")||matchType.equalsIgnoreCase("false")){
									getResponse = reltioAPIService.get(apiUrl+"/"+ objectsToProces.uri +"/_matches?deep=1");								
									
								}else{
									getResponse = reltioAPIService.get(apiUrl+"/"+ objectsToProces.uri +"/_matches");								
								}
//								System.out.println(getResponse);
								
								if (getResponse !=null && getResponse.contains("uri")) {
									
									String drivGetResponse = reltioAPIService.get(apiUrl+"/"+ objectsToProces.uri);
									// Convert the string the java object
									ReltioObject drivReltioObject = GSON.fromJson(drivGetResponse,ReltioObject.class);
																
									//System.out.println(getResponse);
									Map<String, String> responseMap = getXrefResponse(drivReltioObject, attributes);
									
									String[] finalResponse = objectArrayToStringArray(filterMapToObjectArray(responseMap, responseHeader));	
								 
									Set<Integer> keyset=matchUri.keySet();
									for (int mu : keyset ) {
										String quote = "\"";
										getResponse = getResponse.replaceAll(quote + matchUri.get(mu)+quote , quote+mu+quote);
									}
									
									BigMatch mtch = GSON.fromJson(getResponse,new TypeToken<BigMatch>(){}.getType());								
								
									List<Map<String,List<HObject>>> hcoObjectListMap = getMatchName(mtch);
									
									if(hcoObjectListMap!= null && !hcoObjectListMap.isEmpty() ){
						    			for (Map<String,List<HObject>> hcoObjsMap : hcoObjectListMap) {
						    				for (String key : hcoObjsMap.keySet()) {
						    					
						    					if (key!=null ) {
						    						
						    						List<HObject> hcoObjs = hcoObjsMap.get(key);
							    					for(HObject hco: hcoObjs){
							    						
							    						ReltioObject matchReltioObject=hco.object;
						    						
							    						Map<String, String> responseMatchMap = getXrefResponse(matchReltioObject, attributes);
														
														String[] finalMatchResponse = objectArrayToStringArray(filterMapToObjectArray(responseMatchMap, responseHeader));	
														
														String[] matRule= new String[1];
														matRule[0]=key;
														
														String[] merg =ArrayUtils.addAll(matRule, concatArray(finalResponse,finalMatchResponse));
							    						
														reltioFile.writeToFile(merg);
						    						
							    					}
						    					}
						    				}
						    			}
									}	
									
									
								}								
								
								

							} catch (Exception e) {
								e.printStackTrace();
							}
							return requestExecutionTime;
						}
					}));
					
					
					if ( threadNum >= threadsNumber) {
						
						threadNum=0;					
						int completedCount= waitForTasksReady(futures, threadsNumber * 2, threadsNumber);			
						
						if (completedCount >0 ) {
							processedCount=processedCount+completedCount;						
							//System.out.println("Processed records count = "+ processedCount + " Thread size = "+ futures.size());
							
						}	
					
					}
					
					}	

				} else {
					eof = true;
					break;
				}
				scanResponseObj.setObjects(null);
				intitialJSON = GSON.toJson(scanResponseObj.getCursor());

				intitialJSON="{\"cursor\":"+intitialJSON+"}";

			
		}

		waitForTasksReady(futures, 0, threadsNumber * 3);
		executorService.shutdown();
		
		reltioFile.close();
				
		System.out.println("Extract process Completed.....");
		long finalTime = System.currentTimeMillis() - programStartTime;
		System.out.println("[Performance]:  Total processing time : "
				+ (finalTime / 1000) + "  Seconds");

        
 		
	}
	public static int waitForTasksReady(Collection<Future<Long>> futures,
			int maxNumberInList, int threadsNumber) {
		int totalResult = 0;
		if (futures.size() > maxNumberInList) {			
			for (Future<Long> future : new ArrayList<Future<Long>>(futures)) {
				try {					

					future.get();
					totalResult +=1;
					futures.remove(future);		
					
					if (totalResult >= threadsNumber) {
						break;
					}						
					
				} catch (Exception e) {
					e.printStackTrace();
				}
		}			
		}
		return totalResult;
	}	

	
	public static Map<String, String> getXrefResponse(ReltioObject reltioObject, Map<String, InputAttribute> attributes) {

			Map<String, String> responseMap = new HashMap<String, String>();
			responseMap.put("ReltioURI", reltioObject.uri);

			for (Map.Entry<String, InputAttribute> attr : attributes.entrySet()) {
				List<Object> attributeData = reltioObject.attributes.get(attr.getKey());
				ExtractServiceUtil.createExtractOutputData(attr, attributeData, responseMap, null);
			}	
			
		return responseMap;
	}
	
	
	public static Object[] filterMapToObjectArray(final Map<String, ?> values,
			final String[] nameMapping) {

		if (values == null) {
			throw new NullPointerException("values should not be null");
		} else if (nameMapping == null) {
			throw new NullPointerException("nameMapping should not be null");
		}

		final Object[] targetArray = new Object[nameMapping.length];
		int i = 0;
		for (final String name : nameMapping) {
			targetArray[i++] = values.get(name);
		}
		return targetArray;
	}


	public static String[] objectArrayToStringArray(final Object[] objectArray) {
		if (objectArray == null) {
			return null;
		}

		final String[] stringArray = new String[objectArray.length];
		for (int i = 0; i < objectArray.length; i++) {
			stringArray[i] = objectArray[i] != null ? objectArray[i].toString()
					: null;
		}

		return stringArray;
	}
	
	
	
	public static String[] concatArray(String[] src, String[] tgt) {
		String[] finalArr = new String[src.length+tgt.length];
		
		int j=0;
		for (int i=0;i<src.length;i++) {
			finalArr[j++]=src[i];
			finalArr[j++]=tgt[i];
		}		
		return finalArr;
	}
	
	public static String[] concatHeaderArray(String[] src, String[] tgt) {
		String[] finalArr = new String[src.length+tgt.length];
		
		int j=0;
		for (int i=0;i<src.length;i++) {
			finalArr[j++]="src_"+src[i];
			finalArr[j++]="match_"+tgt[i];
		}		
		return finalArr;
	}

	
	public static List<Map<String,List<HObject>>> getMatchName(BigMatch mtch) {		

		List<Map<String,List<HObject>>> hcoObjectListMap = new ArrayList<Map<String,List<HObject>>>();
		if( mtch.hcoObjectList1 != null && !mtch.hcoObjectList1.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(1), mtch.hcoObjectList1);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList2 != null && !mtch.hcoObjectList2.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(2), mtch.hcoObjectList2);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList3 != null && !mtch.hcoObjectList3.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(3), mtch.hcoObjectList3);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList4 != null && !mtch.hcoObjectList4.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(4), mtch.hcoObjectList4);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList5 != null && !mtch.hcoObjectList5.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(5), mtch.hcoObjectList5);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList6 != null && !mtch.hcoObjectList6.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(6), mtch.hcoObjectList6);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList7 != null && !mtch.hcoObjectList7.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(7), mtch.hcoObjectList7);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList8 != null && !mtch.hcoObjectList8.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(8), mtch.hcoObjectList8);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList9 != null && !mtch.hcoObjectList9.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(9), mtch.hcoObjectList9);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList10 != null && !mtch.hcoObjectList10.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(10), mtch.hcoObjectList10);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList11 != null && !mtch.hcoObjectList11.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(11), mtch.hcoObjectList11);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList12 != null && !mtch.hcoObjectList12.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(12), mtch.hcoObjectList12);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList13 != null && !mtch.hcoObjectList13.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(13), mtch.hcoObjectList13);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList14 != null && !mtch.hcoObjectList14.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(14), mtch.hcoObjectList14);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList15 != null && !mtch.hcoObjectList15.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(15), mtch.hcoObjectList15);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList16 != null && !mtch.hcoObjectList16.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(16), mtch.hcoObjectList16);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList17 != null && !mtch.hcoObjectList17.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(17), mtch.hcoObjectList17);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList18 != null && !mtch.hcoObjectList18.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(18), mtch.hcoObjectList18);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList19 != null && !mtch.hcoObjectList19.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(19), mtch.hcoObjectList19);
			hcoObjectListMap.add(hobj);
		}
		
		if( mtch.hcoObjectList20 != null && !mtch.hcoObjectList20.isEmpty() ){
			Map<String,List<HObject>> hobj = new HashMap<String,List<HObject>>();
			hobj.put(matchLabel.get(20), mtch.hcoObjectList20);
			hcoObjectListMap.add(hobj);
		}
		
	
		return hcoObjectListMap;
	}

}
