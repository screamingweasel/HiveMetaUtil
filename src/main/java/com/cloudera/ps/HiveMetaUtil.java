package com.cloudera.ps;
import java.sql.*;
import java.util.*;
import org.json.simple.JSONArray; 
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HiveMetaUtil 
{
	static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"; 
	static JSONParser parser = new JSONParser();
	static Boolean _Partitions = false;
	
	private static Connection openConnection(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			return conn;
	}

	private static JSONObject executeJson (Connection conn, String sql) {
		JSONObject results = null;
		Statement stmt = null;
		ResultSet rs = null;
		String jsonString = null;
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			if (rs.next()) {
				jsonString = rs.getString(1);
				Object obj = parser.parse(jsonString);
				results = (JSONObject) obj;
			}
			rs.close();
			stmt.close();
			
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
				return results;
	}

	private static List<String> getDatabases(Connection conn, String filter) {
		List<String> results = new ArrayList<String>();
		
		JSONObject jdoc = executeJson (conn, "show databases like '" + filter + "'");
		JSONArray jsonArray = (JSONArray) jdoc.get("databases");		
		for (Object o : jsonArray) {
			results.add((String) o);
		}	
		return results;
	}

	private static List<String> getTables(Connection conn, String database, String filter) {
		List<String> results = new ArrayList<String>();	
		
		JSONObject jdoc = executeJson (conn, "show tables in " + database + " like '" + filter + "'");
		JSONArray jsonArray = (JSONArray) jdoc.get("tables");
		for (Object o : jsonArray) {
			results.add((String) o);
		}			
		return results;
	}
	
	private static JSONObject getTable(Connection conn, String database, String table) {
		String sql = "describe extended " + database + "." + table;
		//System.out.println(sql);
		JSONObject results = executeJson (conn, sql);
		//System.out.println(results.toString());
		
		if (_Partitions) {
			JSONArray partitions = getPartitions(conn, results);
		}
		
		return results;
	}
	
	private static JSONArray getPartitions(Connection conn, JSONObject table) {
		JSONArray partitions = new JSONArray();
		String sql = "";

		JSONObject tableInfo = (JSONObject) table.get("tableInfo");
		String dbName = (String) tableInfo.get("dbName");
		String tableName = (String) tableInfo.get("tableName");

		JSONArray partitionKeys = (JSONArray) tableInfo.get("partitionKeys");		
		//System.out.println("partitionKeys=\n" + partitionKeys.toString());

		if (partitionKeys == null) {
			return partitions;
		}
		
		sql = "show partitions " + dbName + "." + tableName;
		JSONObject parts = executeJson (conn, sql);
		//System.out.println("results=\n" + parts.toString());
	
		JSONArray partItems = (JSONArray) parts.get("partitions");
		for (Object o : partItems) {
			JSONObject jo = (JSONObject) o;
			//System.out.println(jo.toString());
			
			sql = "describe extended " + dbName + "." + tableName + " partition (" + (String) jo.get("name") + ")";
			//System.out.println(sql);
			JSONObject partitionInfo = executeJson (conn, sql);
			//System.out.println("-----------------------");
			//System.out.println(partitionInfo.toString());
		}

		return partitionKeys;
	}
	 	   	
    public static void main( String[] args ) { 		
	   	if (args.length < 7) {
	   		System.err.println("Usage <driver> <conn string> <user> <password> <fetch size> <databaseFilter> <tableFilter>");
	   		System.exit(4);
	   	}
		Connection conn = null;

		String JDBC_DRIVER = args[0];
		String DB_URL = args[1];
		String USER =  args[2];
		String PASS =  args[3];
		int fetchSize = Integer.parseInt(args[4]);
		String databaseFilter=args[5];
		String tableFilter=args[6];

		String jsonString;
		JSONObject jdoc;
		JSONArray jsonArray;
		Integer i;
		
		try {
			conn = openConnection(JDBC_DRIVER, DB_URL, USER, PASS);
			jdoc = executeJson (conn, "show databases");
			jsonArray = (JSONArray) jdoc.get("databases");
			
			List<String> databases = getDatabases(conn, databaseFilter);
			i = (Integer) databases.size();
			System.err.println("Found " + i.toString() + " databases");
			for (String database : databases) {
				List<String> tables = getTables(conn, database, tableFilter);
				i = (Integer) tables.size();
				System.err.println("Found " + i.toString() + " tables in database " + database);
				for (String table : tables) {
					System.err.println("Parsing table " + database + "." + table);
					JSONObject tableInfo = getTable(conn, database, table);
					jsonString = tableInfo.toString();
					System.out.println(jsonString);
				}
			}			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
		try {
			if(conn!=null)
				conn.close();
		} catch(SQLException se){
			se.printStackTrace();
			}
		}
	}
}