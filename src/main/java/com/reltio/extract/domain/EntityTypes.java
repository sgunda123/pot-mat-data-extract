package com.reltio.extract.domain;

import java.util.List;

public class EntityTypes {
	
	private String uri;
	
	private List<SurrogateCrosswalks> surrogateCrosswalks;
	
	
	public List<SurrogateCrosswalks> getSurrogateCrosswalks() {
		return surrogateCrosswalks;
	}

	public void setSurrogateCrosswalks(List<SurrogateCrosswalks> surrogateCrosswalks) {
		this.surrogateCrosswalks = surrogateCrosswalks;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	private List<Attribute> matchGroups;

	public List<Attribute> getMatchGroups() {
		return matchGroups;
	}

	public void setMatchGroups(List<Attribute> matchGroups) {
		this.matchGroups = matchGroups;
	}
	
	
	public Attribute startObject;
	public Attribute endObject;

}
