package net.sf.jsqlparser;

import java.sql.SQLException;

import org.junit.Test;

import execute.Execute;

public class JSQLTest {

	String driverClass = "oracle.jdbc.OracleDriver";
	String url = "jdbc:oracle:thin:@localhost:1521:lqjacklee";
	String user = "SYS as sysdba";
	String password = "lqjacklee1A";
	String sql = "select a,b,c,d from t1,t2 where a=3 and b=4 ";

	@Test
	public void test() throws SQLException, ReflectiveOperationException {
		Execute execute2 = new Execute(driverClass, url, user, password);
		execute2.execute(sql, true);
	}
}