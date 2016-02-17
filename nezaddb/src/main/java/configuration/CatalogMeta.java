package configuration;

import gdd.ColumnNotFoundException;
import gdd.MutipleDefitionExeception;

import java.util.List;

import configuration.Configuration.ColumnMeta;
import configuration.Configuration.Description;
import configuration.Configuration.SchemaMeta;

class CatalogMeta extends Description {
	List<SchemaMeta> schemas;

	void add(SchemaMeta schema) {
		schemas.add(schema);
	}

	String getColumnPartName(ColumnMeta column)
			throws ColumnNotFoundException, MutipleDefitionExeception {

		for (SchemaMeta schema : schemas) {
			return schema.getColumnPartName(column);
		}
		return null;
	}
}