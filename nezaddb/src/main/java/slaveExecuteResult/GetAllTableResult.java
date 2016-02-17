package slaveExecuteResult;

public class GetAllTableResult implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String sql;
	private String tableName;
	
	public GetAllTableResult(String tableName,String sql){
		this.tableName = tableName;
		this.sql = sql;
	}
	
	public String getSql(){
		return sql;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public void setSql(String sql){
		this.sql = sql;
	}
	
	public void setTableName(String tableName){
		this.tableName  = tableName;
	}
	
	
	
}
