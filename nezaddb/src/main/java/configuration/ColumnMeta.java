package configuration;


class ColumnMeta extends Description {
	String colType;

	ColumnMeta(String colName, String colType) {
		this.name = colName;
		this.colType = colType;
	}
}