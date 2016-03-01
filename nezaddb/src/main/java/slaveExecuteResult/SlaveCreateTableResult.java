package slaveExecuteResult;

import globalDefinition.CONSTANT;

public class SlaveCreateTableResult extends SlaveExecuteResult{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private boolean success;
	private String errorInfo;
	
	public SlaveCreateTableResult(boolean flag,String errorinfo){
		this.setExecuteType(CONSTANT.SQL_CREATE);
		this.success = flag;
		this.errorInfo = errorinfo;
	}
	
	public boolean getSuccess(){
		return this.success;
	}
	
	public String getErrorInfo(){
		return this.errorInfo;
	}
	
	public void setSuccess(boolean flag){
		this.success = flag;
	}
	
	public void setErrorInfo(String errorinfo){
		this.errorInfo = errorinfo;
	}
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("slaveInsertResult info:");
		if(success){
			System.out.println("create table success");	
		}
		else{
			System.out.println("errorinfo="+this.errorInfo);
		}
		
	}
	
}
