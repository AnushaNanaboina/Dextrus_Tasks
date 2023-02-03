package com.rightdata.task.service;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.rightdata.task.CC;
import com.rightdata.task.entity.TableType;
import com.rightdata.task.entity.AboutTable;
import com.rightdata.task.entity.ConnectionProperties;
import com.rightdata.task.entity.ViewsAndTablesClass;

@Service
public class ConnectionServiceClass {

	public Connection getMSSQLServerConnection(ConnectionProperties connectionProperties) {
		// TODO Auto-generated method stubpublic String
		// connectionStatus(ConnectionProperties connectionProperties) {
		Connection connection = CC.getDBConnection(connectionProperties);
		return connection;
	}

	public List<String> getCatalogsList(ConnectionProperties connectionProperties) {
		// TODO Auto-generated method stub
		List<String> catalogsList = null;
		Connection connection = CC.getDBConnection(connectionProperties);
		try {
			ResultSet rs = connection.createStatement().executeQuery("select name from sys.databases");
			catalogsList = new ArrayList<>();
			while (rs.next()) {
				catalogsList.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return catalogsList;
	}

	public List<String> getSchemasList(ConnectionProperties connectionProperties, String catalog) {
		// TODO Auto-generated method stub
		List<String> schemasList = null;
		Connection connection = CC.getDBConnection(connectionProperties);
		// String query=
		try {
			// PreparedStatement ps=connection.prepareStatement("select name from
			// "+catalog+".sys.schemas");
			// ps.setString(1, catalog);
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery("select name from " + catalog + ".sys.schemas");
			schemasList = new ArrayList<>();
			while (rs.next())
				schemasList.add(rs.getString("name"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return schemasList;
	}

	public List<ViewsAndTablesClass> getViewsAndTablesList(ConnectionProperties connectionProperties, String catalog,
			String schema) {
		// TODO Auto-generated method stub
		List<ViewsAndTablesClass> viewsAndTablesList = new ArrayList<>();
		Connection connection = CC.getDBConnection(connectionProperties);
		try {
			PreparedStatement ps = connection.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_SQL);
			ps.setString(1, catalog);
			ps.setString(2, schema);
			ResultSet rs = ps.executeQuery();
//			Statement st=connection.createStatement();
//			ResultSet rs=st.executeQuery("use "+catalog+"; "+CC.GET_TABLES_SQL);
			while (rs.next()) {
				ViewsAndTablesClass viewsAndTables = new ViewsAndTablesClass();
				viewsAndTables.setTableName(rs.getString("TABLE_NAME"));
				viewsAndTables.setTableType(rs.getString("TABLE_TYPE"));
				viewsAndTablesList.add(viewsAndTables);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return viewsAndTablesList;
	}

	public List<AboutTable> getDescriptionOfTable(ConnectionProperties connectionProperties, String catalogName,
			String schemaName, String tableName) {
		// TODO Auto-generated method stub
		List<AboutTable> tableDescriptionList = new ArrayList<>();
		Connection connection = CC.getDBConnection(connectionProperties);
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement("use " + catalogName + "; " + CC.GET_TABLE_DESC_SQL);
			tableName = schemaName + "." + tableName;
			preparedStatement.setString(1, tableName);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				AboutTable tableDesc = new AboutTable();
				tableDesc.setColumnName(rs.getString("COLUMN_NAME"));
				tableDesc.setDataType(rs.getString("DATA_TYPE"));
				tableDesc.setIsNullable(rs.getInt("IS_NULLABLE"));
				tableDesc.setPrimaryKey(rs.getInt("PRIMARY_KEY"));
				tableDescriptionList.add(tableDesc);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableDescriptionList;
	}

	public List<List<Object>> getTablesData(ConnectionProperties connectionProperties, String query) {
		// TODO Auto-generated method stub
		List<List<Object>> rows = new ArrayList<>();
		Connection connection = CC.getDBConnection(connectionProperties);
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(query);
			ResultSetMetaData metaData = resultSet.getMetaData();
			int countColumn = metaData.getColumnCount();
			while (resultSet.next()) {
//				List<Object> rowList=new ArrayList<>();
				List<Object> row = new ArrayList<>();
				for (int i = 1; i <= countColumn; i++) {
					String columnName = metaData.getColumnName(i);
					String columnType = metaData.getColumnTypeName(i);
					switch (columnType) {
					case "varChar": {
						row.add(columnName + ":" + resultSet.getString(columnName));
						break;
					}
					case "float": {
						row.add(columnName + ":" + resultSet.getFloat(columnName));
						break;
					}
					case "boolean":
						row.add(columnName + ":" + resultSet.getBoolean(columnName));
						break;
					case "int":
						row.add(columnName + ":" + resultSet.getInt(columnName));
						break;
					case "timestamp":
						row.add(columnName + ":" + resultSet.getTimestamp(columnName));
						break;
					case "decimal":
						row.add(columnName + ":" + resultSet.getBigDecimal(columnName));
						break;
					case "date":
						row.add(columnName + ":" + resultSet.getDate(columnName));
						break;
					default:
						row.add("!---!" + columnName + ":" + resultSet.getObject(columnName));
						System.out.println("Datatype not available for column:" + columnName);
					}
				}
				rows.add(row);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rows;
	}

	public List<TableType> getTablesAndViewsByPattern(ConnectionProperties connectionProperties, String catalog,String pattern) {
		// TODO Auto-generated method stub
		List<com.rightdata.task.entity.TableType> viewsAndTablesList = new ArrayList<>();
		Connection connection = CC.getDBConnection(connectionProperties);
		try {
		PreparedStatement preparedStatement = connection
				.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_AND_VIEWS_BY_PATTERN_QUERY);
		preparedStatement.setString(1, pattern);
		ResultSet resultSet = preparedStatement.executeQuery();
		while (resultSet.next()) {
			 com.rightdata.task.entity.TableType tableType=new TableType();
			 tableType.setTable_name(resultSet.getString("TABLE_NAME"));
			 tableType.setTable_type(resultSet.getString("TABLE_TYPE"));
			 viewsAndTablesList.add(tableType);
		}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return viewsAndTablesList;
	}

}
