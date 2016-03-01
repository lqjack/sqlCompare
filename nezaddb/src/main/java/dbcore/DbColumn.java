package dbcore;

public class DbColumn {
	public String table, column, typeName;
	public int type;

	public DbColumn(String table, String column, String typeName) {
		this.table = table;
		this.column = column;
		this.typeName = typeName;
	}

	public String concat(char delimiter) {
		return table + delimiter + column;
	}

	public String toString() {
		return concat('.');
	}
}
