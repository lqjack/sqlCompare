package executeReturnResult;

import java.io.Serializable;

public abstract class ExecuteReturnResult implements Serializable {
    static final long serialVersionUID = 1;
	protected int executeType;
	protected boolean isError;
	protected String errorMsg;
	
	public abstract void displayResult();
	
	public void setExecuteType(int type){
		this.executeType = type;
	}
	
	public int getExecuteType(){
		return this.executeType;
	}
	
	public boolean isError() {
	    return isError;
	}
}
