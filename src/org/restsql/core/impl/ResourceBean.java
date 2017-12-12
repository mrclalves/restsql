package org.restsql.core.impl;

public class ResourceBean {
	
	private Integer id;
	private String name;
	private String resourceXML;

	public ResourceBean() {
	}

	/**
	 * @return the id
	 */
	public Integer getId() {
		return this.id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Integer id) {
		this.id = id;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the resourceXML
	 */
	public String getResourceXML() {
		return this.resourceXML;
	}

	/**
	 * @param resourceXML the resourceXML to set
	 */
	public void setResourceXML(String resourceXML) {
		this.resourceXML = resourceXML;
	}

}
