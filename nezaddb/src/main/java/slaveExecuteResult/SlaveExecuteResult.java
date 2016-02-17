package slaveExecuteResult;

public abstract class SlaveExecuteResult implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int executeType;
	
	public abstract void displayResult();
	
	public void setExecuteType(int type){
		this.executeType = type;
	}
	
	public int getExecuteType(){
		return this.executeType;
	}
}
