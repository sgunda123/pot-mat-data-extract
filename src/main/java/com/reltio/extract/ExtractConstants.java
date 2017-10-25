/**
 * 
 */
package com.reltio.extract;

import com.google.gson.Gson;

/**
 *
 *
 */
public final class ExtractConstants {

	// DB Scan parameters
	public static final String SCAN_URL = "/entities/_dbscan";
	public static final int MAX_PAGE_SIZE = 40;
	public static final String SCAN_INPUT = "{   \"type\" : \"configuration/entityTypes/:EntityType\",\"pageSize\" :"
			+ MAX_PAGE_SIZE + "  }";

	public static final Gson GSON = new Gson();
	public static final String ENCODING = "UTF-8";
	public static final Integer THREAD_COUNT = 10;

}
