package executeResult;

//import parserResult.ParseResultVisitor;

public abstract class ExecuteResult implements java.io.Serializable{
	private int executeType;
	private static final long serialVersionUID = 1L;
	
	public abstract void displayResult();
	
	public void setExecuteType(int type){
		this.executeType = type;
	}
	
	public int getExecuteType(){
		return this.executeType;
	}
	
}
