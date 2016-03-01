package configuration;

import java.util.ArrayList;
import java.util.List;

public class TableMeta extends Description {
	List<ColumnMeta> cols = new ArrayList<ColumnMeta>();
	
	String catalogName;
	String schemaName;
	String tableType;
	
	public TableMeta(String catalogName, String schemaName, String tableName) {
		this.catalogName = catalogName;
		this.schemaName = schemaName;
		this.name = tableName;
	}

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
	
	public String toString(){
		StringBuffer strBuffer = new StringBuffer();
		if(getNotNullString(catalogName)){
			strBuffer.append(catalogName + ".");
		}
		if(getNotNullString(schemaName)){
			strBuffer.append(schemaName +".");
		}
		if(getNotNullString(name)){
			strBuffer.append(name + ".");
		}
		return null;
	}
	
	private boolean getNotNullString(String content){
		if(content == null || content.equals("")) return false;
		return true;
	}
}