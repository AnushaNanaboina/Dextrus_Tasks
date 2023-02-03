package com.rightdata.task;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.rightdata.task.entity.ConnectionProperties;

public class CC {
public static final String GET_TABLES_SQL="SELECT TABLE_NAME,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ?";
public static final String GET_TABLE_DESC_SQL ="SELECT c.name AS 'COLUMN_NAME', t.name AS 'DATA_TYPE', c.is_nullable AS 'IS_NULLABLE', ISNULL(i.is_primary_key, 0) AS 'PRIMARY_KEY' FROM sys.columns c INNER JOIN sys.types t ON c.user_type_id = t.user_type_id LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id LEFT JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id WHERE c.object_id = OBJECT_ID(?)" ;
public static final String GET_TABLES_AND_VIEWS_BY_PATTERN_QUERY = "SELECT TABLE_NAME,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE ?"; 
private static Connection connection;
public static Connection getDBConnection(ConnectionProperties connectionProperties) {
	try {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		Connection connection=DriverManager.getConnection(connectionProperties.getUrl(),connectionProperties.getUsername(),connectionProperties.getPassword());
		return connection;
	} catch (ClassNotFoundException | SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return connection;
	}
}
}
