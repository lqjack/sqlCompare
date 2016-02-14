package executeResult;

import globalDefinition.CONSTANT;

public class CreateDBExecuteResult extends ExecuteResult {
    private static final long serialVersionUID = 1L;
	private String createDBSql;
	
	
	public CreateDBExecuteResult(){
		this.setExecuteType(CONSTANT.SQL_CREATEDATABASE);
	}
	
	public String getSql(){
		return this.createDBSql;
	}
	
	public void setSql(String sql){
		this.createDBSql = sql;
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		
	}	
}
