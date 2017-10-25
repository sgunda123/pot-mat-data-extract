/**
 * 
 */
package com.reltio.extract.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ganesh
 * 
 */
public class XrefInputAttribute {

	public List<String> attributes = new ArrayList<>();
	public Integer count = 1;
	
	public Map<String, List<XrefInputAttribute>> attributesMap = null;
	
	public Map<String, String> uriValuesMap = null;
	
	public Boolean isReference = false;
	
	public List<Crosswalk> refCrosswalks = null;
}
