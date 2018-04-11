/**
 * 
 */
package com.reltio.extract.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.Properties;

import com.reltio.cst.util.GenericUtilityService;
import com.reltio.*;

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
	private String extractAllValues;

	private Integer threadCount;

	public ExtractProperties(Properties properties) {
		// READ the Properties values
		serverHostName = properties.getProperty("RELTIO_SERVER_HOST");
		tenantId = properties.getProperty("TENANT_ID");
		if (!GenericUtilityService.checkNullOrEmpty(serverHostName)
				&& !GenericUtilityService.checkNullOrEmpty(tenantId)) {
			apiUrl = "https://" + serverHostName + "/reltio/api/" + tenantId;
		}

		authUrl = properties.getProperty("AUTH_URL");
		entityType = properties.getProperty("ENTITY_TYPE");
		ovAttrFilePath = properties.getProperty("OV_ATTRIBUTE_FILE_LOCATION");
		xrefAttrFilePath = properties
				.getProperty("XREF_ATTRIBUTE_FILE_LOCATION");
		outputFilePath = properties.getProperty("OUTPUT_FILE_LOCATION");
		username = properties.getProperty("USERNAME");
		password = properties.getProperty("PASSWORD");
		isHeaderRequired = properties.getProperty("HEADER_REQUIRED");
		setTransitive_match(properties.getProperty("TRANSITIVE_MATCH"));
		fileFormat = properties.getProperty("FILE_FORMAT");
		fileDelimiter = properties.getProperty("FILE_DELIMITER");
		extractAllValues = properties.getProperty("EXTRACT_ALL_VALUES");

		if (!GenericUtilityService.checkNullOrEmpty(properties
				.getProperty("THREAD_COUNT"))) {
			threadCount = ExtractConstants.THREAD_COUNT;
		} else {
			threadCount = Integer.parseInt(properties
					.getProperty("THREAD_COUNT"));
		}
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
	 * @return the tenantId
	 */
	public String getExtractAllValues() {
		return extractAllValues;
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

}
