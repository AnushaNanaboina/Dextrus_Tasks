package com.rightdata.task.entity;

public class RequestBodyQuery {
	private ConnectionProperties connectionProperties;
	private String query;
	public ConnectionProperties getConnectionProperties() {
		return connectionProperties;
	}
	public void setConnectionProperties(ConnectionProperties connectionProperties) {
		this.connectionProperties = connectionProperties;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
}
