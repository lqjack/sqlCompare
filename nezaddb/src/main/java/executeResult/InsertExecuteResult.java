package executeResult;

import globalDefinition.CONSTANT;

public class InsertExecuteResult extends ExecuteResult {
    private static final long serialVersionUID = 1L;
	private String siteName;
	private String tableName;
	private String insertSql;
	
	public InsertExecuteResult(String siteName,String tableName,String insertSql){
		this.setExecuteType(CONSTANT.SQL_INSERT);
		this.siteName = siteName;
		this.tableName = tableName;
		this.insertSql = insertSql;
	}
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("siteName="+siteName+",tableName="+tableName+",insertSql="+insertSql);	
	}
	
	public String getSiteName(){
		return siteName;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public String getInsertSql(){
		return insertSql;
	}
	
	public void setSiteName(String sitename){
		this.siteName = sitename;
	}
	
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	public void setInsertSql(String sql){
		this.insertSql = sql;
	}
}
