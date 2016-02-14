package gdd;

import java.util.ArrayList;
import java.util.List;

import com.github.jinahya.sql.database.metadata.bind.Table;

public class TableConver {

	public static TableInfo conver(Table table) {
		TableInfo tableInfo = new TableInfo();
		tableInfo.setTableName(table.getTableName());
		return tableInfo;
	}

	public static List<TableInfo> conver(List<Table> tables) {
		List<TableInfo> tabInfos = new ArrayList<TableInfo>();
		for (Table table : tables)
			tabInfos.add(conver(table));
		return tabInfos;
	}
}
