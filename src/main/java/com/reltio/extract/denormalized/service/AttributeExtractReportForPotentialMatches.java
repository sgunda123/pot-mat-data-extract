package com.reltio.extract.denormalized.service;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.reltio.cst.service.ReltioAPIService;
import com.reltio.cst.util.Util;
import com.reltio.extract.domain.Attribute;
import com.reltio.extract.domain.Configuration;
import com.reltio.extract.domain.EntityTypes;
import com.reltio.extract.domain.ExtractProperties;
import com.reltio.extract.domain.InputAttribute;
import com.reltio.extract.domain.ReltioObject;
import com.reltio.extract.domain.ScanResponse;
import com.reltio.extract.util.ExtractServiceUtil;
import com.reltio.file.ReltioFileWriter;



/**
 * 
 * This is the main class for generating the Extract Report
 * 
 * @author Mohan
 * 
 */
public class AttributeExtractReportForPotentialMatches {
	private static final Logger LOGGER = LogManager.getLogger(AttributeExtractReportForPotentialMatches.class.getName());
	private static final Logger logPerformance = LogManager.getLogger("performance-log");

	private static Gson GSON = new Gson();
	private static final String[] DefaultAttributes = { "ReltioURI"};
	private static Map<String,String> matchRules = null;
	public static void main(String[] args) throws Exception {

		LOGGER.info("Extract Process Started..");
		long programStartTime = System.currentTimeMillis();
		Properties config = Util.getProperties(args[0], "PASSWORD", "CLIENT_CREDENTIALS");

		Map<List<String>, List<String>> mutualExclusiveProps = new HashMap<>();
		mutualExclusiveProps.put(Arrays.asList("PASSWORD","USERNAME"), Arrays.asList("CLIENT_CREDENTIALS"));
		List<String> missingKeys = Util.listMissingProperties(config,
				Arrays.asList("ENTITY_TYPE","ENVIRONMENT_URL", "AUTH_URL", "FILE_FORMAT", "ENVIRONMENT_URL", "TENANT_ID", "OUTPUT_FILE"), mutualExclusiveProps);


		if (!missingKeys.isEmpty()) {
			System.out.println(
					"Following properties are missing from configuration file!! \n" + String.join("\n", missingKeys));
			System.exit(0);
		}

		// READ the Properties values
		final ExtractProperties extractProperties = new ExtractProperties(
				config);

		Util.setHttpProxy(config);

		String targetRuleName;
		if(extractProperties.getTargetRule() != null && !extractProperties.getTargetRule().isEmpty())
		{
			targetRuleName = extractProperties.getTargetRule();
		}
		else {
			targetRuleName = "AllRules";
		}


		final ReltioAPIService reltioAPIService = Util.getReltioService(config);


		String configRes = reltioAPIService.get(extractProperties.getApiUrl()+"/configuration/_noInheritance");
		Configuration configObj = GSON.fromJson(configRes,Configuration.class);
		matchRules = new LinkedHashMap<String,String>();

		for (EntityTypes entT : configObj.getEntityTypes()) {

			if (entT.getUri().equals("configuration/entityTypes/"+extractProperties.getEntityType())) {

				if (entT.getMatchGroups() !=null && !entT.getMatchGroups().isEmpty()) {
					/*
					 * Creating map of matchrules uri and label
					 */
					for (Attribute attr : entT.getMatchGroups() ) {

						matchRules.put(attr.getUri().trim(), attr.getLabel().trim());

					}
				}
			}
		}


		int threadsNumber = extractProperties.getThreadCount();	
		Integer count = 0;
		long processedCount = 0l;
		final int MAX_QUEUE_SIZE_MULTIPLICATOR = 1;

		final Map<String, InputAttribute> attributes = new LinkedHashMap<>();		

		String selectAttributes = null;
		// Read OV Values Attribute
		if (extractProperties.getOvAttrFilePath() != null
				&& !extractProperties.getOvAttrFilePath().isEmpty()
				&& !extractProperties.getOvAttrFilePath().equalsIgnoreCase(
						"null")) {

			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(extractProperties.getOvAttrFilePath()), "UTF-8"));
			selectAttributes = ExtractServiceUtil.createAttributeMapFromProperties(reader,
					attributes);

			Util.close(reader);

		}

		final List<String> fileResponseHeader = new ArrayList<String>();

		for (String header : DefaultAttributes) {
			fileResponseHeader.add(header);
		}

		// Create Response file header
		for (Map.Entry<String, InputAttribute> attr : attributes.entrySet()) {
			ExtractServiceUtil.createExtractNestedResponseHeader(attr,fileResponseHeader, null);
		}

		final String[] responseHeader = new String[fileResponseHeader.size()];
		fileResponseHeader.toArray(responseHeader);

		final HashMap<String,ReltioFileWriter> reltioFilesMap = new HashMap<>();

		String[] matRuleHead= new String[1];
		matRuleHead[0]="Rule";			
		String[] finHeaderArray =(String[])ArrayUtils.addAll(matRuleHead, concatHeaderArray (responseHeader,responseHeader));

		final String apiUrl =  extractProperties.getApiUrl();

		//If target rule is specified filter search by match rules
		String filterUrl = consructFilterUrl(extractProperties, targetRuleName);
		final String scanUrl = apiUrl + "/entities/_scan?"+filterUrl+"&select=uri&max="+extractProperties.getNoOfRecordsPerCall();

		String incReportURLTotal = apiUrl + "/entities/_total?"+filterUrl;
		LOGGER.info("Total=="+reltioAPIService.get(incReportURLTotal));  
		LOGGER.info(scanUrl);
		String intitialJSON = "";

		ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadsNumber);
		boolean eof = false;
		ArrayList<Future<Long>> futures = new ArrayList<Future<Long>>();

		while (!eof) {

			for (int i = 0; i < threadsNumber * MAX_QUEUE_SIZE_MULTIPLICATOR; i++) {

				// Doing the DBScan API Call here
				String scanResponse = reltioAPIService.post(scanUrl,intitialJSON);

				// Convert the string the java object
				ScanResponse scanResponseObj = GSON.fromJson(scanResponse,ScanResponse.class);

				if (scanResponseObj.getObjects() != null && scanResponseObj.getObjects().size() > 0 && (extractProperties.getSampleSize() > count || extractProperties.getSampleSize() == 0))  {

					count += scanResponseObj.getObjects().size();
					LOGGER.info("Scaned records count = " + count);

					final List<ReltioObject> objectsToProcess = scanResponseObj.getObjects();	

					PMExtractTask ec = new PMExtractTask(objectsToProcess, reltioAPIService, extractProperties, apiUrl, attributes, responseHeader, matchRules, reltioFilesMap, finHeaderArray, selectAttributes, targetRuleName);
					Future<Long> f = executorService.submit(ec);
					futures.add(f);


				}
				else {
					eof = true;
					break;
				}

				scanResponseObj.setObjects(null);
				intitialJSON = GSON.toJson(scanResponseObj.getCursor());

				intitialJSON="{\"cursor\":"+intitialJSON+"}";
			}

			processedCount += waitForTasksReady(futures, threadsNumber*(MAX_QUEUE_SIZE_MULTIPLICATOR / 2));			


			printPerformanceLog(executorService.getCompletedTaskCount()
					* extractProperties.getNoOfRecordsPerCall(),
					processedCount,
					programStartTime,
					threadsNumber);

		}

		processedCount += waitForTasksReady(futures, 0);
		executorService.shutdown();
		
		for(Map.Entry<String, ReltioFileWriter> entry : reltioFilesMap.entrySet()) {
			Util.close(entry.getValue());
		}
		if(processedCount > 0) {
			printPerformanceLog(executorService.getCompletedTaskCount()
					* extractProperties.getNoOfRecordsPerCall(),
					processedCount,
					programStartTime,
					threadsNumber);
		}

		LOGGER.info("Extract process Completed.....");


		long finalTime = System.currentTimeMillis() - programStartTime;
		logPerformance.info("[Performance]:  Total processing time : " + 
				finalTime / 1000L + "  Seconds");

	}

	public static void printPerformanceLog(long totalTasksExecuted,
			long totalTasksExecutionTime,
			long programStartTime, long numberOfThreads) {
		
		StringBuilder performanceLogBuilder = new StringBuilder(); 
		performanceLogBuilder.append("\n[Performance]: ============= Current performance status (%s) ============= \n [Performance]:  Total processing time : %s \n [Performance]:  Match Pairs extracted for Entities: %s ")
				.append("\n [Performance]:  Total OPS (Match Pairs extracted for Entities / Time spent from program start):  %s \n [Performance]:  Total OPS without waiting for queue (Match Pairs extracted for Entities / (Time spent from program start - Time spent in waiting for API queue)): %s")
				.append("\n [Performance]:  API Server data load requests OPS (Match Pairs extracted for Entities / (Sum of time spend by API requests / Threads count)):  %s \n [Performance]: =============================================================================================================== \n");

		
		long finalTime = System.currentTimeMillis() - programStartTime;

		String performanceLog = performanceLogBuilder.toString();
		LOGGER.info(String.format(performanceLog, new Date().toString(), finalTime, totalTasksExecuted, (totalTasksExecuted / (finalTime / 1000f)), 
				(totalTasksExecuted / ((finalTime) / 1000f)), (totalTasksExecuted / ((totalTasksExecutionTime / numberOfThreads) / 1000f))));

		logPerformance.info(String.format(performanceLog, new Date().toString(), finalTime, totalTasksExecuted, (totalTasksExecuted / (finalTime / 1000f)), 
				(totalTasksExecuted / ((finalTime) / 1000f)), (totalTasksExecuted / ((totalTasksExecutionTime / numberOfThreads) / 1000f))));
	}

	/**
	 * Waits for futures (load tasks list put to executor) are partially ready.
	 * <code>maxNumberInList</code> parameters specifies how much tasks could be
	 * uncompleted.
	 *
	 * @param futures         - futures to wait for.
	 * @param maxNumberInList - maximum number of futures could be left in "undone" state.
	 * @return sum of executed futures execution time.
	 */
	public static long waitForTasksReady(Collection<Future<Long>> futures,
			int maxNumberInList) {
		long totalResult = 0l;
		while (futures.size() > maxNumberInList) {
			try {
				Thread.sleep(20);
			} catch (Exception e) {
				LOGGER.error("Ignoring the error");
			}
			
			Iterator<Future<Long>> itr = futures.iterator();
			while (itr.hasNext()) {
				Future<Long> future = itr.next();
				if (future.isDone()) {
					try {
						totalResult += future.get();
						itr.remove();
					} catch (Exception e) {
						LOGGER.error("Ignoring the error");
					}
				}
			}
		}
		return totalResult;
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

	private static String consructFilterUrl(ExtractProperties extractProperties, String targetRuleName) {
		StringBuilder urlBuilder = new StringBuilder();

		urlBuilder.append("filter=equals(type,'configuration/entityTypes/");
		urlBuilder.append(extractProperties.getEntityType());
		urlBuilder.append("') and range(matches,");
		urlBuilder.append(extractProperties.getMin());
		urlBuilder.append(",");
		urlBuilder.append(extractProperties.getMax());
		urlBuilder.append(")");

		if(!targetRuleName.equals("AllRules")) {
			urlBuilder.append("and equals(matchRules,'");
			urlBuilder.append(targetRuleName).append("')");
		}

		
		if(extractProperties.getFilterCondition() != null && !extractProperties.getFilterCondition().isEmpty()) {
			urlBuilder.append(" and ").append(extractProperties.getFilterCondition());

		}

		return urlBuilder.toString();
	}

}