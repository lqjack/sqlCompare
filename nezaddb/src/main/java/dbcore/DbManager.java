package dbcore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

import configuration.Configuration;
import executeResult.ImportExecuteResultUnit;

public class DbManager {
	private int maxBatch = 3000;
	private String database = null;
	private Connection conn = null;

	public DbManager() {
		String dbname = Configuration.getInstance().getOption(
				"SiteConfiguration.DatabaseConfiguration.dbname");
		conn = DbConnection.getDBConnection(dbname);
		try {
			this.executeQuery("set max_heap_table_size=65535000");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
	}

	public DbManager(String database) {
		this.database = database;
		conn = DbConnection.getDBConnection(database);
	}

	public void setMaxBatch(int max) {
		maxBatch = max;
	}

	public void setDatabase(String database) {
		this.database = database;
		if (conn != null) {
			DbConnection.closeConnection(conn);
		}
		conn = DbConnection.getDBConnection(this.database);
	}

	public boolean createDB(String dbname) {
		String sql = "create database " + dbname;
		try {
			executeCreateDB(sql);
		} catch (SQLException ex) {
			System.out.println("createDB:" + ex.toString());
			return false;
		}
		return true;
	}

	public boolean dropDB(String dbname) {
		String sql = "drop database " + dbname;
		try {
			executeCreateDB(sql);
		} catch (SQLException ex) {
			System.out.println("dropDB:" + ex.toString());
			return false;
		}
		return true;
	}

	public boolean executeCreateDB(String sql) throws SQLException {
		Statement stmt = null;
		boolean result;
		try {
			stmt = conn.createStatement();
			result = stmt.execute(sql);
		} catch (SQLException ex) {
			throw ex;
		} finally {
			stmt.close();
		}

		return result;
	}

	public boolean executeQuery(String sql) throws SQLException {
		Statement stmt = null;
		boolean result;

		try {
			stmt = conn.createStatement();
			result = stmt.execute(sql);
		} catch (SQLException ex) {
			throw ex;
		} finally {
			stmt.close();
		}

		return result;
	}

	public boolean executeCreateTable(String sql) throws SQLException {
		return executeQuery(sql);
	}

	public synchronized boolean createTempTable(String name, String[] attrList,
			String[] types) throws SQLException {
		String createTempTable = null;
		if (attrList.length != types.length)
			return false;
		int len = attrList.length;
		createTempTable = "CREATE TABLE " + name + " (";
		for (int i = 0; i < len - 1; i++) {
			createTempTable += attrList[i] + " " + types[i] + ",";
		}
		createTempTable += attrList[len - 1] + " " + types[len - 1];
		createTempTable += ")";

		executeCreateTable(createTempTable);

		return true;
	}

	public synchronized void createTableLike(String table, String like)
			throws SQLException {
		this.executeCreateTable("create table " + table + " like " + like);
	}

	public synchronized void dropTable(String name) throws SQLException {
		String sql = "drop table " + name;
		executeQuery(sql);
	}

	public boolean executeImportTable(ImportExecuteResultUnit unit)
			throws SQLException {
		Statement stmt = null;
		// int[] result = null;
		String sql;
		Vector<String> sqls = new Vector<String>();

		try {
			stmt = conn.createStatement();

			for (int i = 0; i < unit.columnInfos.size(); i++) {

				sql = "insert into " + unit.tableName + unit.columnNameString
						+ " values" + unit.columnInfos.elementAt(i);
				sqls.add(sql);
			}

			stmt = conn.createStatement();
			conn.setAutoCommit(false);
			for (int i = 0; i < sqls.size(); i++) {
				stmt.addBatch(sqls.elementAt(i));
				if ((i + 1) % maxBatch == 0) {
					stmt.executeBatch();
				}
			}
			conn.commit();
			conn.setAutoCommit(true);

		} catch (SQLException ex) {
			throw ex;
		} finally {
			stmt.close();
		}

		return false;
	}

	public int executeInsert(String sql) throws SQLException {
		Statement stmt = null;
		int result;

		try {
			stmt = conn.createStatement();
			result = stmt.executeUpdate(sql);
		} catch (SQLException ex) {
			throw ex;
		} finally {
			stmt.close();
		}
		return result;
	}

	public synchronized DbTable executeSelect(String query) throws SQLException {
		DbTable table = new DbTable();
		// Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// conn = DbConnection.getDBConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			table.initTable(rs);
		} catch (SQLException ex) {
			throw ex;
		} finally {
			// rs.close();
			stmt.close();
			// DbConnection.closeConnection(conn);
		}
		return table;
	}

	public synchronized void selectIntoTable(String name, String[] attrList,
			String select) throws SQLException {
		String query = "INSERT INTO " + name + " (";
		for (int i = 0; i < attrList.length - 1; i++) {
			query += attrList[i] + ",";
		}
		query += attrList[attrList.length - 1] + ") " + select;
		executeQuery(query);
	}

	public synchronized DbColumn[] getSelectResultColumns(String query)
			throws SQLException {
		query += " limit 0";
		Statement stmt = null;
		ResultSet rs = null;
		DbColumn[] attrList = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			attrList = getAllColumns(rs);
		} catch (SQLException ex) {
			throw ex;
		} finally {
			rs.close();
			stmt.close();
		}
		return attrList;
	}

	private DbColumn[] getAllColumns(ResultSet rs) throws SQLException {
		DbColumn[] attrList;
		ResultSetMetaData meta = rs.getMetaData();
		int colCount = meta.getColumnCount();
		attrList = new DbColumn[colCount];
		for (int i = 1; i <= colCount; i++) {
			attrList[i - 1] = new DbColumn(meta.getTableName(i),
					meta.getColumnName(i), meta.getColumnTypeName(i));
		}
		return attrList;
	}

	public synchronized void createIndex(String table, String column)
			throws SQLException {
		this.executeQuery("create index " + table + "_" + column
				+ "_idx using btree on " + table + "(" + column + ")");
	}

	public synchronized void importData(String name, String[] attrList,
			ArrayList<ArrayList<Object>> rows) throws SQLException {
		long start = System.currentTimeMillis();
		BufferedWriter writer = null;
		String path = "";
		File outfile = new File(name + System.currentTimeMillis());
		path = outfile.getAbsolutePath().replace('\\', '/');
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outfile)));
			for (int i = 0; i < rows.size(); i++) {
				ArrayList<Object> row = rows.get(i);
				for (int j = 0; j < row.size(); j++) {
					writer.write(row.get(j).toString());
					writer.write("\t");
				}
				writer.write("\n");
			}
			writer.close();
		} catch (IOException ex) {

		}
		executeQuery("load data infile '" + path + "' into table " + name
				+ " FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		if (outfile.exists()) {
			outfile.delete();
		}
		long end = System.currentTimeMillis();
		System.out.println("import table : " + (end - start) + "ms");
	}

	public synchronized void insertBatch(String name, String[] attrList,
			ArrayList<ArrayList<Object>> rows) throws SQLException {
		long start = System.currentTimeMillis();
		String insertHead = "insert into " + name + "(";
		char insertEnd = ')';
		for (int i = 0; i < attrList.length - 1; i++) {
			insertHead += attrList[i] + ",";
		}
		insertHead += attrList[attrList.length - 1] + ") values (";

		Iterator<ArrayList<Object>> it = rows.iterator();
		Vector<String> queries = new Vector<String>();
		while (it.hasNext()) {
			ArrayList<Object> row = it.next();
			StringBuffer sql = new StringBuffer(insertHead);
			for (int i = 0; i < row.size() - 1; i++) {
				sql.append('\'');
				sql.append(row.get(i).toString().replaceAll("'", "\\\\'"));
				sql.append('\'');
				sql.append(',');
			}
			sql.append('\'');
			sql.append(row.get(row.size() - 1).toString()
					.replaceAll("'", "\\\\'"));
			sql.append('\'');
			sql.append(insertEnd);
			queries.add(sql.toString());
		}
		long end = System.currentTimeMillis();
		System.out.println("insertBatch : " + (end - start));
		start = System.currentTimeMillis();
		executeExecuteBatch(queries);
		end = System.currentTimeMillis();
		System.out
				.println("executeBatch : " + (end - start) + " with "
						+ rows.size() + " records and " + attrList.length
						+ " columns.");
	}

	public int[] executeExecuteBatch(Vector<String> queries)
			throws SQLException {
		// Connection conn = null;
		Statement stmt = null;
		int[] result = null;

		try {
			stmt = conn.createStatement();
			conn.setAutoCommit(false);
			for (int i = 0; i < queries.size(); i++) {
				stmt.addBatch(queries.elementAt(i));
				if ((i + 1) % maxBatch == 0) {
					result = stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException ex) {
			conn.rollback();
			throw ex;
		} finally {
			stmt.close();
			// DbConnection.closeConnection(conn);
		}
		return result;
	}

	public void closeDb() {
		DbConnection.closeConnection(conn);
	}

	public static void main(String args[]) {
		DbManager dbm = new DbManager();
		String[] attrList = { "id", "title", "author", "pid", "copies" };
		String[] types = { "int", "char(100)", "varchar(200)", "int", "int" };
		ArrayList<ArrayList<Object>> val = new ArrayList<ArrayList<Object>>();
		String sql = "select * from orders1";
		sql = "select * from orders1 o1, orders1 o2 where o1.book_id = o2.book_id and o1.book_id = 1";

		for (int i = 0; i < 5; i++) {
			ArrayList<Object> row = new ArrayList<Object>();
			row.add(i);
			row.add("a'bc");
			row.add("test");
			val.add(row);
		}

		try {
			File outfile = new File("temp");
			String path = outfile.getAbsolutePath();
			System.out.println(path.replace('\\', '/'));
			dbm.setDatabase("ddbtest");
			dbm.executeQuery("set max_heap_table_size=268435456");
			DbTable t = dbm.executeSelect("show variables like '%max_heap%'");
			System.out.println(t.rowAt(0));
			dbm.createTempTable("temp1", attrList, types);

			long start = System.currentTimeMillis();
			t = dbm.executeSelect("select * from book");
			BufferedWriter writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("testbook")));
				ArrayList<ArrayList<Object>> list = t.getRows();
				for (int i = 0; i < list.size(); i++) {
					ArrayList<Object> row = list.get(i);
					for (int j = 0; j < row.size(); j++) {
						writer.write(row.get(j).toString());
						writer.write("\t");
					}
					writer.write("\n");
				}
				writer.close();
			} catch (Exception ex) {

			}
			dbm.executeQuery("load data infile 'E:/Project/nezaddb/testbook' into table temp1 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
			t = dbm.executeSelect("select * from temp1");
			long end = System.currentTimeMillis();
			System.out.println(t.getRowCount() + "@" + (end - start) + "ms");
			// dbm2.createTempTable("temp1", attrList, types);
			// dbm.insertBatch("temp1", attrList, val);
			// dbm.dropTable("temp1");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
	}
}
