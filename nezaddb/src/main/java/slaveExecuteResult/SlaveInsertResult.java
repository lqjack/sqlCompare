package slaveExecuteResult;

import globalDefinition.CONSTANT;

public class SlaveInsertResult extends SlaveExecuteResult{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int affectedNum;
	private boolean success;
	private String errorInfo;
	
	public SlaveInsertResult(int affected,boolean flag,String errorinfo){
		this.setExecuteType(CONSTANT.SQL_INSERT);
		this.affectedNum = affected;
		this.success = flag;
		this.errorInfo = errorinfo;
	}
	
	public int getAffectedNum(){
		return this.affectedNum;
	}
	
	public boolean getSuccess(){
		return this.success;
	}
	
	public String getErrorInfo(){
		return this.errorInfo;
	}
	
	public void setAffectedNum(int num){
		this.affectedNum = num;
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
			System.out.println("affectedNum="+this.affectedNum);	
		}
		else{
			System.out.println("errorinfo="+this.errorInfo);
		}
	}

}
