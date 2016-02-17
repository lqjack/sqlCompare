package configuration;

import gdd.ColumnNotFoundException;
import gdd.MutipleDefitionExeception;

import java.util.List;

import configuration.Configuration.ColumnMeta;
import configuration.Configuration.Description;
import configuration.Configuration.TableMeta;

class SchemaMeta extends Description {
	List<TableMeta> tables;

	void add(TableMeta tableInfo) {
		tables.add(tableInfo);
	}

	String getColumnPartName(ColumnMeta column)
			throws ColumnNotFoundException, MutipleDefitionExeception {
		int foundNum = 0;
		String fullColumnName = "";
		for (TableMeta tab : tables) {
			fullColumnName = tab.getColumnPartName(column);
			if (fullColumnName != null && !fullColumnName.equals(""))
				foundNum++;
		}
		if (foundNum < 1)
			throw new ColumnNotFoundException(column.name
					+ " not found under the schema : " + this.name);
		if (foundNum > 1)
			throw new MutipleDefitionExeception(column.name
					+ " have mutiple definition found under the schema : "
					+ this.name);
		return fullColumnName;
	}
}
