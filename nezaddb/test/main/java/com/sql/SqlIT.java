package com.sql;

import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.Assert;

import org.apache.commons.dbutils.DbUtils;
import org.culturegraph.mf.sql.connection.ConnectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SqlIT {

	Connection conn;
	/**
	 * jdbc:oracle:thin:user/xxxx@server:port:SID
		jdbc:oracle:thin:user/xxxx@//server:port/XE
		jdbc:oracle:thin:user/xxxx@:SID
	 */
	@Before
	public void before() {
		String driverClass = "oracle.jdbc.OracleDriver";
		String url = "jdbc:oracle:thin:@localhost:1521:lqjacklee";
		String user = "SYS as sysdba";
		String password = "lqjacklee1A";
		conn = ConnectionUtils.getConnection(driverClass, url, user, password);
		Assert.assertNotNull(conn);
	}
	
	@Test
	public void test(){
		Assert.assertNotNull(conn);
	}
	
	@After
	public void after(){
		try {
			DbUtils.close(conn);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
