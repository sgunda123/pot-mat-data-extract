package com.reltio.extract.domain;

public class Attribute {

	private String uri;
	private String label;
	private String value;
	private String cursor;
	
	public String objectTypeURI;
	
	
	
	
	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	private String displayName;

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * @return the cursor
	 */
	public String getCursor() {
		return cursor;
	}

	/**
	 * @param cursor
	 *            the cursor to set
	 */
	public void setCursor(String cursor) {
		this.cursor = cursor;
	}

	/**
	 * @return the displayName
	 */
	public String getDisplayName() {
		return displayName;
	}

	/**
	 * @param displayName
	 *            the displayName to set
	 */
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

}
