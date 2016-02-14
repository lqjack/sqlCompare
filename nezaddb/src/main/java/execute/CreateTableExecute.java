package execute;

import java.util.List;
import gdd.ColumnInfo;
import gdd.GDD;
import gdd.TableInfo;
import parserResult.CreateTableResult;
import parserResult.ParseResult;

public class CreateTableExecute extends ExecuteSQL {
	
	private GDD gdd;
	public CreateTableExecute(){
		gdd = GDD.getInstance();
	}
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		CreateTableResult createTableResult = (CreateTableResult)result;
		String tablename = createTableResult.getTableName();
		if(gdd.isTableExist(tablename)){
			System.out.println(tablename + " is exist now! can't create again!");
			System.exit(-1);
		}
		
		TableInfo table = new TableInfo();
		table.setTableName(tablename);
		table.setDBName(gdd.getDBName());
		
		
		List<CreateTableResult.TableColumn> columns = createTableResult.getColomns();
		table.setColNum(columns.size());
		for(int i = 0 ; i < columns.size() ; i++){
			CreateTableResult.TableColumn col = columns.get(i);
			ColumnInfo column = new ColumnInfo(col.columnName,i,col.columnType,1-col.isKey,col.isKey,col.maxLength);
			table.getColumnInfo().add(column);	
		}
		gdd.getTableInfos().add(table);
		gdd.addTableNum();
		table.setTableID(gdd.getTableNum());	
	}	
}
