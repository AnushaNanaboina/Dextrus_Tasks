package com.rightdata.task.entity;

public class RequestBodyPattern {
private ConnectionProperties connectionProperties;
private String pattern;
private String catalog;
public ConnectionProperties getConnectionProperties() {
	return connectionProperties;
}
public void setConnectionProperties(ConnectionProperties connectionProperties) {
	this.connectionProperties = connectionProperties;
}
public String getPattern() {
	return pattern;
}
public void setPattern(String pattern) {
	this.pattern = pattern;
}
public String getCatalog() {
	return catalog;
}
public void setCatalog(String catalog) {
	this.catalog = catalog;
}
}
