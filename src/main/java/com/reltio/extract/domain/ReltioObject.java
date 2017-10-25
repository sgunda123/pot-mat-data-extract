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

}
