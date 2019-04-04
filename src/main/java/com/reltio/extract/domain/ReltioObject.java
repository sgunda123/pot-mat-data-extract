package com.reltio.extract.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Ganesh.Palanisamy@reltio.com Created : Sep 19, 2014
 */
public class ReltioObject {

	public String type;
	public String uri;
	public String label;
	
	public Map<String, List<Object>> attributes = new HashMap<String, List<Object>>();
	
	public List<Crosswalk> crosswalks;

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @param label the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * @return the attributes
	 */
	public Map<String, List<Object>> getAttributes() {
		return attributes;
	}

	/**
	 * @param attributes the attributes to set
	 */
	public void setAttributes(Map<String, List<Object>> attributes) {
		this.attributes = attributes;
	}

	/**
	 * @return the crosswalks
	 */
	public List<Crosswalk> getCrosswalks() {
		return crosswalks;
	}

	/**
	 * @param crosswalks the crosswalks to set
	 */
	public void setCrosswalks(List<Crosswalk> crosswalks) {
		this.crosswalks = crosswalks;
	}

}
