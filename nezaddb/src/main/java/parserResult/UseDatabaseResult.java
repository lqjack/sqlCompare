package parserResult;

import globalDefinition.CONSTANT;

public class UseDatabaseResult extends ParseResult{
	private String databaseName;
	public UseDatabaseResult(String databaseName){
		this.databaseName = databaseName;
		setSqlType(CONSTANT.SQL_USEDATABASE);	
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("usedatabase parse Result:");
		System.out.println(databaseName);
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

}
