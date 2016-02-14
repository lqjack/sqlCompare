package executeResult;

import java.util.List;
import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;

public class DeleteExecuteResult extends ExecuteResult {
    private static final long serialVersionUID = 1L;
	private String siteName;
	private String tableName;
	private int fragType;
	private String deleteSql;
	private List<SimpleExpression> expressions;
	
	public DeleteExecuteResult(String siteName,String tableName,String deleteSql){
		this.setExecuteType(CONSTANT.SQL_DELETE);
		this.siteName = siteName;
		this.tableName = tableName;
		this.deleteSql = deleteSql;
	}
	
	public DeleteExecuteResult(String siteName,String tableName,int fragType,String deleteSql,List<SimpleExpression> expressions){
		this.setExecuteType(CONSTANT.SQL_DELETE);
		this.siteName = siteName;
		this.tableName = tableName;
		this.fragType = fragType;
		this.deleteSql = deleteSql;
		this.expressions = expressions;
	}
	
	public String getSiteName(){
		return siteName;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public String getDeleteSql(){
		return deleteSql;
	}
	
	public int getFragType(){
		return this.fragType;
	}
	
	public List<SimpleExpression> getExpressions(){
		return this.expressions;
	}
	
	public void setSiteName(String sitename){
		this.siteName = sitename;
	}
	
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	public void setDeleteSql(String sql){
		this.deleteSql = sql;
	}
	
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("DeleteExecuteResult info:");
		if(this.fragType == CONSTANT.FRAG_HORIZONTAL)
			System.out.println("siteName="+siteName+",tableName="+tableName+",deleteSql="+deleteSql);
		else if(this.fragType == CONSTANT.FRAG_VERTICAL){
			System.out.println("siteName="+siteName+",tableName="+tableName);
			for(int i = 0 ; i < this.expressions.size() ; i++){
				SimpleExpression expression = this.expressions.get(i);
				expression.displayResult();
			}
		}
	}

}
