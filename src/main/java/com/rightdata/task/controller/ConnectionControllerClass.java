package com.rightdata.task.controller;

import java.sql.Connection;
import java.util.List;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.rightdata.task.entity.AboutTable;
import com.rightdata.task.entity.ConnectionProperties;
import com.rightdata.task.entity.RequestBodyPattern;
import com.rightdata.task.entity.RequestBodyQuery;
import com.rightdata.task.entity.TableType;
import com.rightdata.task.entity.ViewsAndTablesClass;
import com.rightdata.task.service.ConnectionServiceClass;

@RestController
@RequestMapping()
public class ConnectionControllerClass {
	@Autowired
	private ConnectionServiceClass connectionService;

	@PostMapping("/connect")
	public ResponseEntity<String> getConnection(@RequestBody ConnectionProperties connectionProperties) {
		Connection connection = connectionService.getMSSQLServerConnection(connectionProperties);
		if (connection == null)
			return new ResponseEntity<String>("Not connected", HttpStatus.SERVICE_UNAVAILABLE);
		else
			return new ResponseEntity<String>("Connected to MS SQL Server", HttpStatus.OK);
	}
	@GetMapping("/catalogs")
	public ResponseEntity<List<String>> getCatalogsList(@RequestBody ConnectionProperties connectionProperties){
		List<String> catalogsList=connectionService.getCatalogsList(connectionProperties);
		return new ResponseEntity<List<String>>(catalogsList,HttpStatus.OK);
}
@GetMapping("/{catalog}")
public ResponseEntity<List<String>> getSchemasList(@RequestBody ConnectionProperties connectionProperties,@PathVariable String catalog){
	List<String> schemasList=connectionService.getSchemasList(connectionProperties,catalog);
	return new ResponseEntity<List<String>>(schemasList,HttpStatus.OK);	
}
@GetMapping("/{catalog}/{schema}")
public ResponseEntity<List<ViewsAndTablesClass>> getViewsAndTablesList(@RequestBody ConnectionProperties connectionProperties,@PathVariable String catalog,@PathVariable String schema){
	List<ViewsAndTablesClass> viewsAndTablesList=connectionService.getViewsAndTablesList(connectionProperties,catalog,schema);
	System.out.println("Views and Tables size:"+viewsAndTablesList.size());
	return new ResponseEntity<List<ViewsAndTablesClass>>(viewsAndTablesList,HttpStatus.OK);
	}
@GetMapping("/{catalogName}/{schemaName}/{tableName}")
public ResponseEntity<List<AboutTable>> getColumnsAndProperties(@RequestBody ConnectionProperties connectionProperties,@PathVariable String catalogName,@PathVariable String schemaName,@PathVariable String tableName ){
	List<AboutTable> aboutTableList=connectionService.getDescriptionOfTable(connectionProperties,catalogName,schemaName,tableName);
	return new ResponseEntity<List<AboutTable>>(aboutTableList,HttpStatus.OK);
}
@GetMapping("/metadata")
public ResponseEntity<List<List<Object>>> getTableData(@RequestBody RequestBodyQuery queryBody){
	ConnectionProperties connectionProperties=queryBody.getConnectionProperties();
	String query=queryBody.getQuery();
	List<List<Object>> tableDataList=connectionService.getTablesData(connectionProperties,query);
	return new ResponseEntity<List<List<Object>>>(tableDataList,HttpStatus.OK); 
}
@PostMapping("/searchstring")
public ResponseEntity<List<TableType>> getTablesAndViewsByPattern(@RequestBody RequestBodyPattern requestBodyPattern){
	List<TableType> viewsAndTablesList=connectionService.getTablesAndViewsByPattern(requestBodyPattern.getConnectionProperties(),requestBodyPattern.getCatalog(),requestBodyPattern.getPattern());
	return new ResponseEntity<List<TableType>>(viewsAndTablesList,HttpStatus.OK);
}
}

