package parserResult;

import globalDefinition.CONSTANT;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class CreateTableResult extends ParseResult{
	public class TableColumn{
		public String columnName;
		public int isKey; //1 as key and 0 not
		public int columnType;
		public int maxLength; //useful when columnType is String
		public TableColumn(){
			isKey = 0;
			columnType = -1;
			maxLength = 0;
		}
	}
	private String tableName;
	private List<?> columnDefinitions;
	private List<?> tableOptionsStrings;
	private List<?> indexes;
	private List<TableColumn> columns;
	public CreateTableResult(String tableName,List<?> columnDefinitions,List<?> tableOptionsStrings,List indexes){
		setSqlType(CONSTANT.SQL_CREATE);
		this.tableName = tableName;
		this.columnDefinitions = columnDefinitions;
		this.tableOptionsStrings = tableOptionsStrings;
		this.indexes = indexes;
		genColumns();
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		
		System.out.println("Create table parse result:");
		System.out.println(tableName);
		for(int i=0;i<columns.size();++i){
			System.out.print(columns.get(i).columnName);
			System.out.print(" "+ ((columns.get(i).columnType==1)?"int":"char"));
			System.out.print(" "+columns.get(i).maxLength);
			System.out.println(" "+((columns.get(i).isKey == 1)?"key":""));
		}
	}
	private void genColumns(){
		if(columnDefinitions == null) return;
		columns = new ArrayList<TableColumn>();
		for(int i=0;i<columnDefinitions.size();++i){
			ColumnDefinition col  = (ColumnDefinition) columnDefinitions.get(i);
			ColDataType d = col.getColDataType();
			TableColumn newCol =  new TableColumn();
			newCol.columnName = col.getColumnName();
			if(d.getDataType().equalsIgnoreCase("int")){
				newCol.columnType = CONSTANT.VALUE_INT;
			}
			else if(d.getDataType().equalsIgnoreCase("char")){
				newCol.columnType = CONSTANT.VALUE_STRING;
			}
			else if(d.getDataType().equalsIgnoreCase("double")){
				newCol.columnType = CONSTANT.VALUE_DOUBLE;
			}
			if(d.getArgumentsStringList()!=null){
				newCol.maxLength = Integer.parseInt((String)d.getArgumentsStringList().get(0));	
			}
			if(col.getColumnSpecStrings()!=null)
				newCol.isKey = 1;
			columns.add(newCol);
		}
		
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setColomns(List<TableColumn> columns) {
		this.columns = columns;
	}

	public List<TableColumn> getColomns() {
		return columns;
	}

}
