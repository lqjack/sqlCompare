package configuration;

import java.util.List;

import configuration.Configuration.ColumnMeta;
import configuration.Configuration.Description;

class TableMeta extends Description {
	List<ColumnMeta> cols;

	void add(ColumnMeta col) {
		cols.add(col);
	}

	String getColumnPartName(ColumnMeta column) {
		for (ColumnMeta col : cols) {
			if (column.name.equals(col.name))
				return name + "." + column.name;
		}
		return null;
	}
}