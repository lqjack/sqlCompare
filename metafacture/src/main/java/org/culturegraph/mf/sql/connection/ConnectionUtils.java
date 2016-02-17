package org.culturegraph.mf.sql.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbutils.DbUtils;

public class ConnectionUtils {

	private static void loadDriver(String driverClassName) {
		DbUtils.loadDriver(driverClassName);
	}

	public static Connection getConnection(String url) {
		return getConnection(url, null);
	}

	public static Connection getConnection(String url, Properties info) {
		try {
			return DriverManager.getConnection(url, info);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Connection getConnection(String url, String user, String password) {
		//TODO the user and password will vary depend on database implements
		Properties params = new Properties();
		params.put("user", user);//TODO should be varied
		params.put("password", password);//TODO should be varied
		return getConnection(url, params);
	}
	
	public static Connection getConnection(String driverClass,String url, String user, String password) {
		loadDriver(driverClass);
		return getConnection(url,user,password);
	}
}
