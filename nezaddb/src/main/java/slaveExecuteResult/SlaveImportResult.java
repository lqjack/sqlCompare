package slaveExecuteResult;

import globalDefinition.CONSTANT;

public class SlaveImportResult extends SlaveExecuteResult{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private boolean success;
	private String errorInfo;

	
	public SlaveImportResult(boolean success,String errorinfo){
		this.setExecuteType(CONSTANT.SQL_IMPORTDATA);
		this.success = success;
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
		if(this.errorInfo != null)
			System.out.println(this.errorInfo);
		
	}

}
