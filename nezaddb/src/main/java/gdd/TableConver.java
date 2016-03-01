package gdd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import configuration.TableMeta;

public class TableConver {

	public static TableInfo conver(TableMeta table) {
		TableInfo tableInfo = new TableInfo();
		tableInfo.setTableName(table.toString());
		return tableInfo;
	}

	public static Collection<TableInfo> conver(List<TableMeta> tables){
		Collection<TableInfo> tis = new ArrayList<TableInfo>();
		for(TableMeta tm : tables)
			tis.add(conver(tm));
		return tis;
	}
}
