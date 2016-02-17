package configuration;

import configuration.Configuration.Description;

class ColumnMeta extends Description {
	String colType;

	ColumnMeta(String colName) {
		this.name = colName;
	}
}