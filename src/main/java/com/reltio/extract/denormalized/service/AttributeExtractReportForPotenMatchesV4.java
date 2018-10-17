package com.reltio.extract.denormalized.service;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang3.ArrayUtils;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.reltio.cst.service.ReltioAPIService;
import com.reltio.cst.service.TokenGeneratorService;
import com.reltio.cst.service.impl.SimpleReltioAPIServiceImpl;
import com.reltio.cst.service.impl.TokenGeneratorServiceImpl;
import com.reltio.extract.domain.Attribute;
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
		private static Map<Integer,String> matchUri = null;
		private static Map<Integer,String> matchLabel = null;
		public static void main(String[] args) throws Exception {
			
			System.out.println("Extract Process Started..");
			long programStartTime = System.currentTimeMillis();
			Properties properties = new Properties();
			
			try {
				String propertyFilePath = args[0]; 
				FileReader in = new FileReader(propertyFilePath);
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
						.println("Error::: one or more required parameters missing. Please verify the input properties File...." );
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
									Map<String, String> responseMap = getXrefResponse(drivReltioObject, attributes, extractProperties );
									
									String[] finalResponse = objectArrayToStringArray(filterMapToObjectArray(responseMap, responseHeader));	

									JSONObject object = (JSONObject)GSON.fromJson(getResponse, new TypeToken<JSONObject>() {  } .getType());
									if(object!= null && !object.isEmpty() ){
										Iterator<String> objItr = object.keySet().iterator();
										
										while (objItr.hasNext()) {
											String ruleName = objItr.next();
											ArrayList<HObject> objects = GSON.fromJson(GSON.toJson(((List)object.get(ruleName))), new TypeToken<ArrayList<HObject>>() {  } .getType());

											for(HObject obj: objects){

					    						ReltioObject matchReltioObject = obj.object;
				    						
					    						Map<String, String> responseMatchMap = getXrefResponse(matchReltioObject, attributes, extractProperties);

												//System.out.println(getXrefResponse(matchReltioObject, attributes));
												//Does not have all names here
												String[] finalMatchResponse = objectArrayToStringArray(filterMapToObjectArray(responseMatchMap, responseHeader));	

												String[] matRule= new String[1];
												matRule[0]=ruleName;
												
												String[] merg =ArrayUtils.addAll(matRule, concatArray(finalResponse,finalMatchResponse));
					    						
												reltioFile.writeToFile(merg);
				    						
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

	
	public static Map<String, String> getXrefResponse(ReltioObject reltioObject, Map<String, InputAttribute> attributes, ExtractProperties extractProperties) {

			Map<String, String> responseMap = new HashMap<String, String>();
			responseMap.put("ReltioURI", reltioObject.uri);



			for (Map.Entry<String, InputAttribute> attr : attributes.entrySet()) {
				List<Object> attributeData = reltioObject.attributes.get(attr.getKey());


                boolean extractAllValues = false;
                if (extractProperties.isExtractAllValues()
                        ) {
                    extractAllValues = true;

                }
                ExtractServiceUtil.createExtractOutputData(attr, attributeData, responseMap, null, extractAllValues);
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

}
