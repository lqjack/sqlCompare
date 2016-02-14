package executeResult;

import globalDefinition.CONSTANT;

import java.util.Vector;

public class CreateTableExecuteResult extends ExecuteResult implements java.io.Serializable{
    private static final long serialVersionUID = 1L;
	private Vector<String> createTableSqls;
	
	public CreateTableExecuteResult(){
		this.createTableSqls = new Vector<String>();
		this.setExecuteType(CONSTANT.SQL_CREATE);
		
	}
	
	public void addSql(String sql){
		if(this.createTableSqls == null)
			this.createTableSqls = new Vector<String>();
		this.createTableSqls.add(sql);
	}
	
	public void setSql(Vector<String> sqls){
		this.createTableSqls = sqls;
	}
	
	public Vector<String> getSql(){
		return this.createTableSqls;
	}
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		for(int i = 0 ; i < createTableSqls.size() ; i++)
			System.out.println(this.createTableSqls.elementAt(i));
		
	}

}
