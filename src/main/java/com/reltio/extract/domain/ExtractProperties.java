/**
 * 
 */
package com.reltio.extract.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.Properties;

import com.reltio.cst.util.GenericUtilityService;

/**
 *
 *
 */
public class ExtractProperties implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6543973273697083071L;

	private String apiUrl;
	private String authUrl;
	private String entityType;
	private String ovAttrFilePath;
	private String xrefAttrFilePath;
	private String outputFilePath;
	private String username;
	private String password;
	private String isHeaderRequired;
	private String fileFormat;
	private String fileDelimiter;
	private String serverHostName;
	private String tenantId;
	private String transitive_match;
	private boolean extractAllValues;
	private String targetRule;
	private boolean extractPerRule;
	private String filterCondition;

	private int min = 1;
	private int max = 100000;

	private int limit = 200;
	
	private int sampleSize;
	private int noOfRecordsPerCall = 100;

	private Integer threadCount;

	public ExtractProperties(Properties properties) {
		// READ the Properties values
		serverHostName = properties.getProperty("ENVIRONMENT_URL");
		tenantId = properties.getProperty("TENANT_ID");
		if (!GenericUtilityService.checkNullOrEmpty(serverHostName)
				&& !GenericUtilityService.checkNullOrEmpty(tenantId)) {
			apiUrl = "https://" + serverHostName + "/reltio/api/" + tenantId;
		}

		authUrl = properties.getProperty("AUTH_URL");
		entityType = properties.getProperty("ENTITY_TYPE");
		ovAttrFilePath = properties.getProperty("OV_ATTRIBUTE_FILE");
		xrefAttrFilePath = properties
				.getProperty("XREF_ATTRIBUTE_FILE_LOCATION");
		outputFilePath = properties.getProperty("OUTPUT_FILE");
		username = properties.getProperty("USERNAME");
		password = properties.getProperty("PASSWORD");
		isHeaderRequired = properties.getProperty("HEADER_REQUIRED");
		setTransitive_match(properties.getProperty("TRANSITIVE_MATCH"));
		fileFormat = properties.getProperty("FILE_FORMAT");
		fileDelimiter = properties.getProperty("FILE_DELIMITER");
		
		if(properties.getProperty("EXTRACT_ALL_VALUES") != null && !properties.getProperty("EXTRACT_ALL_VALUES").trim().isEmpty()) {
			setExtractAllValues(Boolean.valueOf(properties.getProperty("EXTRACT_ALL_VALUES")));
		}

		targetRule = properties.getProperty("TARGET_RULE");

		if (!GenericUtilityService.checkNullOrEmpty(properties
				.getProperty("SAMPLE_SIZE"))) {
			sampleSize = Integer.parseInt(properties.getProperty("SAMPLE_SIZE"));
		}
		if (GenericUtilityService.checkNullOrEmpty(properties
				.getProperty("THREAD_COUNT"))) {
			threadCount = ExtractConstants.THREAD_COUNT;
		} else {
			threadCount = Integer.parseInt(properties
					.getProperty("THREAD_COUNT"));
		}
		
		
		if (!GenericUtilityService.checkNullOrEmpty(properties
				.getProperty("RECORDS_PER_POST"))) {
			noOfRecordsPerCall = Integer.parseInt(properties
					.getProperty("RECORDS_PER_POST"));
		}


		if(properties.getProperty("EXTRACT_PER_RULE") != null && !properties.getProperty("EXTRACT_PER_RULE").trim().isEmpty()) {
			setExtractPerRule(Boolean.valueOf(properties.getProperty("EXTRACT_PER_RULE")));
		}
		
		filterCondition = properties.getProperty("FILTER_CONDITION");

	}

	public ExtractProperties(String serverHostName, String tenantId,
			String authUrl, String entityType, String username,
			String password, String fileFormat, String fileDelimiter,
			String isHeaderRequired) {

		// READ the Properties values
		this.serverHostName = serverHostName;
		this.tenantId = tenantId;

		if (!GenericUtilityService.checkNullOrEmpty(serverHostName)
				&& !GenericUtilityService.checkNullOrEmpty(tenantId)) {
			this.apiUrl = "https://" + serverHostName + "/reltio/api/"
					+ tenantId;
		}

		this.authUrl = authUrl;
		this.entityType = entityType;

		this.username = username;
		this.password = password;
		this.isHeaderRequired = isHeaderRequired;
		this.fileFormat = fileFormat;
		this.fileDelimiter = fileDelimiter;

		threadCount = ExtractConstants.THREAD_COUNT;

		this.outputFilePath = "/tmp/"+serverHostName + "_" + tenantId + "_"
				+ entityType + "_" + username + "_" + new Date().getTime();
		if (fileFormat.equalsIgnoreCase("CSV")) {
			this.outputFilePath += ".csv";
		} else {
			this.outputFilePath += ".txt";
		}

	}

	/**
	 * @return the extractPerRule
	 */
	public boolean isExtractPerRule() {
		return extractPerRule;
	}

	/**
	 * @param extractPerRule the extractPerRule to set
	 */
	public void setExtractPerRule(boolean extractPerRule) {
		this.extractPerRule = extractPerRule;
	}

	/**
	 * @return the filterCondition
	 */
	public String getFilterCondition() {
		return filterCondition;
	}

	/**
	 * @param filterCondition the filterCondition to set
	 */
	public void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
	}

	/**
	 * @return the min
	 */
	public int getMin() {
		return min;
	}

	/**
	 * @param min the min to set
	 */
	public void setMin(int min) {
		this.min = min;
	}

	/**
	 * @return the max
	 */
	public int getMax() {
		return max;
	}

	/**
	 * @param max the max to set
	 */
	public void setMax(int max) {
		this.max = max;
	}

	/**
	 * @return the limit
	 */
	public int getLimit() {
		return limit;
	}

	/**
	 * @param limit the limit to set
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}

	/**
	 * @param sampleSize the sampleSize to set
	 */
	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}

	/**
	 * @return the apiUrl
	 */
	public String getApiUrl() {
		return apiUrl;
	}

	/**
	 * @param apiUrl
	 *            the apiUrl to set
	 */
	public void setApiUrl(String apiUrl) {
		this.apiUrl = apiUrl;
	}

	/**
	 * @return the authUrl
	 */
	public String getAuthUrl() {
		return authUrl;
	}

	/**
	 * @param authUrl
	 *            the authUrl to set
	 */
	public void setAuthUrl(String authUrl) {
		this.authUrl = authUrl;
	}

	/**
	 * @return the entityType
	 */
	public String getEntityType() {
		return entityType;
	}

	/**
	 * @param entityType
	 *            the entityType to set
	 */
	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	/**
	 * @return the ovAttrFilePath
	 */
	public String getOvAttrFilePath() {
		return ovAttrFilePath;
	}

	/**
	 * @param ovAttrFilePath
	 *            the ovAttrFilePath to set
	 */
	public void setOvAttrFilePath(String ovAttrFilePath) {
		this.ovAttrFilePath = ovAttrFilePath;
	}

	/**
	 * @return the xrefAttrFilePath
	 */
	public String getXrefAttrFilePath() {
		return xrefAttrFilePath;
	}

	/**
	 * @param xrefAttrFilePath
	 *            the xrefAttrFilePath to set
	 */
	public void setXrefAttrFilePath(String xrefAttrFilePath) {
		this.xrefAttrFilePath = xrefAttrFilePath;
	}

	/**
	 * @return the outputFilePath
	 */
	public String getOutputFilePath() {
		return outputFilePath;
	}

	/**
	 * @param outputFilePath
	 *            the outputFilePath to set
	 */
	public void setOutputFilePath(String outputFilePath) {
		this.outputFilePath = outputFilePath;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username
	 *            the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the isHeaderRequired
	 */
	public String getIsHeaderRequired() {
		return isHeaderRequired;
	}

	/**
	 * @param isHeaderRequired
	 *            the isHeaderRequired to set
	 */
	public void setIsHeaderRequired(String isHeaderRequired) {
		this.isHeaderRequired = isHeaderRequired;
	}

	/**
	 * @return the fileFormat
	 */
	public String getFileFormat() {
		return fileFormat;
	}

	/**
	 * @param fileFormat
	 *            the fileFormat to set
	 */
	public void setFileFormat(String fileFormat) {
		this.fileFormat = fileFormat;
	}

	/**
	 * @return the fileDelimiter
	 */
	public String getFileDelimiter() {
		return fileDelimiter;
	}

	/**
	 * @param fileDelimiter
	 *            the fileDelimiter to set
	 */
	public void setFileDelimiter(String fileDelimiter) {
		this.fileDelimiter = fileDelimiter;
	}

	/**
	 * @return the serverHostName
	 */
	public String getServerHostName() {
		return serverHostName;
	}

	/**
	 * @param serverHostName
	 *            the serverHostName to set
	 */
	public void setServerHostName(String serverHostName) {
		this.serverHostName = serverHostName;
	}

	/**
	 * @return the tenantId
	 */
	public String getTenantId() {
		return tenantId;
	}

	

	/**
	 * @param tenantId
	 *            the tenantId to set
	 */
	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	/**
	 * @return the threadCount
	 */
	public Integer getThreadCount() {
		return threadCount;
	}

	/**
	 * @param threadCount
	 *            the threadCount to set
	 */
	public void setThreadCount(Integer threadCount) {
		this.threadCount = threadCount;
	}

	public String getTransitive_match() {
		return transitive_match;
	}

	public void setTransitive_match(String transitive_match) {
		this.transitive_match = transitive_match;
	}

	/**
	 * @return the noOfRecordsPerCall
	 */
	public int getNoOfRecordsPerCall() {
		return noOfRecordsPerCall;
	}

	/**
	 * @param noOfRecordsPerCall the noOfRecordsPerCall to set
	 */
	public void setNoOfRecordsPerCall(int noOfRecordsPerCall) {
		this.noOfRecordsPerCall = noOfRecordsPerCall;
	}

	/**
	 * @return the targetRule
	 */
	public String getTargetRule() {
		return targetRule;
	}

	/**
	 * @param targetRule
	 *            the targetRule to set
	 */
	public void setTargetRule(String targetRule) {
		this.targetRule = targetRule;
	}

	/**
	 * @return the sampleSize
	 */
	public Integer getSampleSize() {
		return sampleSize;
	}

	/**
	 * @param sampleSize
	 *            the threadCount to set
	 */
	public void setSampleSize(Integer sampleSize) {
		this.sampleSize = sampleSize;
	}

	/**
	 * @return the extractAllValues
	 */
	public boolean isExtractAllValues() {
		return extractAllValues;
	}

	/**
	 * @param extractAllValues the extractAllValues to set
	 */
	public void setExtractAllValues(boolean extractAllValues) {
		this.extractAllValues = extractAllValues;
	}

}