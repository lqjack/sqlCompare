package configuration;

import gdd.ColumnNotFoundException;
import gdd.MutipleDefitionExeception;

import java.util.List;

import configuration.Configuration.CatalogMeta;
import configuration.Configuration.ColumnMeta;
import configuration.Configuration.Description;

class SiteMeta extends Description {
	List<CatalogMeta> catalogs;

	void add(CatalogMeta cat) {
		catalogs.add(cat);
	}

	String getColumnPartName(ColumnMeta column)
			throws ColumnNotFoundException, MutipleDefitionExeception {
		for (CatalogMeta cat : catalogs) {
			return cat.getColumnPartName(column);
		}
		return null;
	}
}