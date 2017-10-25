package com.reltio.extract.domain;

import java.util.List;

public class Configuration {

	private List<EntityTypes> entityTypes;
	private List<EntityTypes> relationTypes;
	public List<EntityTypes> getRelationTypes() {
		return relationTypes;
	}

	public void setRelationTypes(List<EntityTypes> relationTypes) {
		this.relationTypes = relationTypes;
	}

	private List<EntityTypes> sources;

	public List<EntityTypes> getSources() {
		return sources;
	}

	public void setSources(List<EntityTypes> sources) {
		this.sources = sources;
	}

	public List<EntityTypes> getEntityTypes() {
		return entityTypes;
	}

	public void setEntityTypes(List<EntityTypes> entityTypes) {
		this.entityTypes = entityTypes;
	}
	
}
