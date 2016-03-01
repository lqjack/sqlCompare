package configuration;

import gdd.ColumnNotFoundException;
import gdd.MutipleDefitionExeception;

import java.util.Map;
import java.util.Map.Entry;

class AllSite {
	Map<String, SiteMeta> sites;

	void add(SiteMeta site) {
		sites.put(site.name, site);
	}

	String getFullName(ColumnMeta column) throws ColumnNotFoundException,
			MutipleDefitionExeception {
		int foundNum = 0;
		String fullColumnName = "";
		for (Entry<String, SiteMeta> entrySite : sites.entrySet()) {
			fullColumnName = entrySite.getValue().getColumnPartName(column);
			if (fullColumnName != null && !fullColumnName.equals(""))
				foundNum++;
		}
		if (foundNum < 1)
			throw new ColumnNotFoundException(column.name + " not found ");
		if (foundNum > 1)
			throw new MutipleDefitionExeception(column.name
					+ " have mutiple definition ");
		return fullColumnName;
	}
}