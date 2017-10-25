package com.reltio.extract.domain;

import java.util.List;

public class ScanResponse {

	private Attribute cursor;
	
	//TODO: Replace Object class to our existing Java Class here
	private List<ReltioObject> objects;

	/**
	 * @return the cursor
	 */
	public Attribute getCursor() {
		return cursor;
	}

	/**
	 * @param cursor the cursor to set
	 */
	public void setCursor(Attribute cursor) {
		this.cursor = cursor;
	}

	/**
	 * @return the objects
	 */
	public List<ReltioObject> getObjects() {
		return objects;
	}

	/**
	 * @param objects the objects to set
	 */
	public void setObjects(List<ReltioObject> objects) {
		this.objects = objects;
	}
	
	
}
