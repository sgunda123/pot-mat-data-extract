/**
 * 
 */
package com.reltio.extract.denormalized.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.reltio.cst.service.ReltioAPIService;
import com.reltio.extract.domain.ExtractConstants;
import com.reltio.extract.domain.ExtractProperties;
import com.reltio.extract.domain.HObject;
import com.reltio.extract.domain.InputAttribute;
import com.reltio.extract.domain.ReltioObject;
import com.reltio.extract.util.ExtractServiceUtil;
import com.reltio.file.ReltioCSVFileWriter;
import com.reltio.file.ReltioFileWriter;
import com.reltio.file.ReltioFlatFileWriter;

/**
 * @author sanjay
 *
 */
public class PMExtractTask implements Callable<Long>{
	private static final Logger LOGGER = LogManager.getLogger(PMExtractTask.class.getName());

	private static Gson GSON = new Gson();
	private static final String OFFSET_LIMIT = "offset=%s&max=%s";

	private List<ReltioObject> objectsToProcess;
	private ReltioAPIService reltioAPIService;
	private ExtractProperties extractProperties;
	private Map<String, InputAttribute> attributes;
	private String apiUrl;
	private String[] responseHeader;
	private Map<String,String> matchRules; 
	private HashMap<String,ReltioFileWriter> reltioFilesMap;
	private String[] finHeaderArray;
	private String selectAttributes;
	private String targetRuleName;

	/**
	 * 
	 */
	public PMExtractTask(List<ReltioObject> objectsToProcess, ReltioAPIService reltioAPIService, ExtractProperties extractProperties
			, String apiUrl, Map<String, InputAttribute> attributes, String[] responseHeader, Map<String,String> matchRules,
			HashMap<String,ReltioFileWriter> reltioFilesMap, String[] finHeaderArray, String selectAttributes, String targetRuleName) {
		this.reltioAPIService = reltioAPIService;
		this.objectsToProcess = objectsToProcess;
		this.extractProperties = extractProperties;
		this.attributes = attributes;
		this.apiUrl = apiUrl;
		this.responseHeader = responseHeader;
		this.matchRules = matchRules;
		this.reltioFilesMap = reltioFilesMap;
		this.finHeaderArray = finHeaderArray;
		this.selectAttributes = selectAttributes;
		this.targetRuleName = targetRuleName;
	}

	@Override
	public Long call() throws Exception {

		long startTime = System.currentTimeMillis();
		long requestExecutionTime = 0l;

		for (final ReltioObject objectsToProces : objectsToProcess) {	

			try {
				String getResponse ="";

				int offset = 0;
				int limit = extractProperties.getLimit();
				boolean matchPresent = true;
				while (matchPresent) {

					if(!extractProperties.isFetchTransitiveMatches()){
						getResponse = reltioAPIService.get(apiUrl+"/"+ objectsToProces.uri +"/_matches?deep=1&"+String.format(OFFSET_LIMIT, offset, limit)+"&select="+getSelectFields());								

					}else{
						getResponse = reltioAPIService.get(apiUrl+"/"+ objectsToProces.uri +"/_matches?"+String.format(OFFSET_LIMIT, offset, limit)+"&select="+getSelectFields());								
					}

					LOGGER.info("Scanning matches for "+objectsToProces.uri+ " with Max =  " + limit+",  offset = "+offset);
					offset = offset+limit;

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
								List<HObject> objects = GSON.fromJson(GSON.toJson(((List)object.get(ruleName))), new TypeToken<ArrayList<HObject>>() {  } .getType());
								if (targetRuleName.equals("AllRules") || ruleName.equalsIgnoreCase(targetRuleName)) {
									for(HObject obj: objects){

										ReltioObject matchReltioObject = obj.object;

										Map<String, String> responseMatchMap = getXrefResponse(matchReltioObject, attributes, extractProperties);

										//System.out.println(getXrefResponse(matchReltioObject, attributes));
										//Does not have all names here
										String[] finalMatchResponse = objectArrayToStringArray(filterMapToObjectArray(responseMatchMap, responseHeader));	

										String[] matRule= {matchRules.get(ruleName)};

										String[] merg =ArrayUtils.addAll(matRule, concatArray(finalResponse,finalMatchResponse));

										getFileToWrite(matRule[0]).writeToFile(merg);

									}
								}

							}
						}	

					}else {
						matchPresent = false;
						break;
					}									

				}

			} catch (Exception e) {
				LOGGER.error("Error while processing the Protential Matches. Object URI = "+objectsToProces.uri, e);
			}
		}
		requestExecutionTime = System.currentTimeMillis()
				- startTime;
		return requestExecutionTime;


	}

	/**
	 * Construct the Select Fields to be passed to the API 
	 * @return
	 */
	private String getSelectFields() {
		StringBuilder selectAttrsBuilder = new StringBuilder();
		selectAttrsBuilder.append("uri,");
		selectAttrsBuilder.append(selectAttributes);
		return selectAttrsBuilder.toString();
	}

	/**
	 * Get the file writer 
	 * @param name
	 * @return
	 * @throws IOException
	 */
	private ReltioFileWriter getFileToWrite(String name) throws IOException {
		final ReltioFileWriter reltioFile;

		if(!extractProperties.isExtractPerRule() && reltioFilesMap.size() > 0) {
			return reltioFilesMap.get("Default");
		}else if(reltioFilesMap.containsKey(name)) {
			return reltioFilesMap.get(name);
		}

		synchronized(reltioFilesMap){
			String filePath = null;
			if(!extractProperties.isExtractPerRule()) {
				filePath = extractProperties.getOutputFilePath();
			}else {
				
				String fileName = name.replaceAll("[^0-9a-zA-Z]+", "_");
				filePath = extractProperties.getOutputFilePath().substring(0, extractProperties.getOutputFilePath().lastIndexOf(File.separator))+File.separator+fileName+".csv";
			}


			// Output File
			// check whether its CSV out FILE or Flat File
			if (extractProperties.getFileFormat().equalsIgnoreCase("CSV")) {
				reltioFile = new ReltioCSVFileWriter(filePath);
			} else if (extractProperties.getFileDelimiter() != null) {
				// provided file Delimiter
				reltioFile = new ReltioFlatFileWriter(filePath,
						ExtractConstants.ENCODING,
						extractProperties.getFileDelimiter());
			} else {

				// Default delimiter pipe
				reltioFile = new ReltioFlatFileWriter(
						extractProperties.getOutputFilePath());
			}						


			if (extractProperties.getIsHeaderRequired() == null
					|| extractProperties.getIsHeaderRequired().equalsIgnoreCase(
							"Yes")) {
				reltioFile.writeToFile(finHeaderArray);
			}

			if(extractProperties.isExtractPerRule()) {
				reltioFilesMap.put(name, reltioFile);
			}else {
				reltioFilesMap.put("Default", reltioFile);
			}
		}


		return reltioFile;
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

	/**
	 * @return the selectAttributes
	 */
	public String getSelectAttributes() {
		return selectAttributes;
	}

	/**
	 * @param selectAttributes the selectAttributes to set
	 */
	public void setSelectAttributes(String selectAttributes) {
		this.selectAttributes = selectAttributes;
	}



}
