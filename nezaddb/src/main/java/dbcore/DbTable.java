package dbcore;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class DbTable implements Serializable {
	private static final long serialVersionUID = 1L;
	private ArrayList<ArrayList<Object>> rows;
	// Table meta data
	private ArrayList<String> colName, tableName;
	private int[] types;
	private String[] typeNames;
	private int colNumOfKey;

	public DbTable() {
		rows = new ArrayList<ArrayList<Object>>();
		colName = new ArrayList<String>();
		tableName = new ArrayList<String>();
		colNumOfKey = -1;
	}

	public int getKey() {
		return colNumOfKey;
	}

	public void setKey(int key) {
		colNumOfKey = key;
	}

	public void initTable(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int numColumns = meta.getColumnCount();
		types = new int[numColumns];
		typeNames = new String[numColumns];
		for (int i = 1; i <= numColumns; i++) {
			colName.add(meta.getColumnName(i));
			tableName.add(meta.getTableName(i));
			types[i - 1] = meta.getColumnType(i);
			typeNames[i - 1] = meta.getColumnTypeName(i);
		}

		// System.out.println(colName);
		while (rs.next()) {
			ArrayList<Object> list = new ArrayList<Object>();
			for (int i = 1; i <= numColumns; i++) {
				// Column numbers start at 1.
				// Also there are many methods on the result set to
				// return
				// the column as a particular type. Refer to the Sun
				// documentation
				// for the list of valid conversions.
				Object o = rs.getObject(i);
				list.add(o);
				// System.out.println("COLUMN " + i + " = " + o.getClass());
			}
			rows.add(list);
		}

	}

	public int getRowCount() {
		return rows.size();
	}

	public ArrayList<Object> rowAt(int index) {
		return rows.get(index);
	}

	public ArrayList<ArrayList<Object>> getRows() {
		return rows;
	}

	public ArrayList<String> getColName() {
		return colName;
	}

	public ArrayList<String> getTableName() {
		return tableName;
	}

	public int[] getType() {
		return types;
	}

	public String[] getTypeNames() {
		return typeNames;
	}

	public static void main(String[] args) {
		DbTable table = null;
		DbManager dbm = new DbManager();

		dbm.setDatabase("shopinfo");
		try {
			table = dbm
					.executeSelect("select * from salesinfo inner join inventory where salesinfo.itemid = inventory.id");
			System.out.println(table.getTableName());
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
	}
}
