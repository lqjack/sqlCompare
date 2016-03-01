package globalDefinition;

public class SimpleExpression implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String tableName;
	public String columnName;
	public String op;
	public String value; 
	public int valueType;
	
	public SimpleExpression(){
		
	}
	public boolean equals(SimpleExpression e){
		if(!tableName.equals(e.tableName))
			return false;
		if(!columnName.equals(e.columnName))
			return false;
		if(!op.equals(e.op))
			return false;
		if(!value.equals(e.value))
			return false;
		if(valueType!=e.valueType)
			return false;
		return true;
	}
	public SimpleExpression(String tableName,String columnName,String op,String value,int valueType){
		this.tableName = tableName;
		this.columnName = columnName;
		this.op = op;
		this.value = value;
		this.valueType = valueType;	
	}
	
	public SimpleExpression(SimpleExpression express){
		if(express.tableName != null)
			this.tableName = express.tableName;
		else
			this.tableName = "NULL";
		
		if(express.columnName != null)
			this.columnName = express.columnName;
		else
			this.columnName = "NULL";
		this.op = express.op;
		this.value = express.value;
		this.valueType = express.valueType;
	}
	
	public void displayResult(){
		System.out.println(this.tableName + "." + this.columnName + this.op + this.value);
	}
	
	public String strExpression() {
	    return this.tableName + "." + this.columnName + this.op + this.value;
	}
}