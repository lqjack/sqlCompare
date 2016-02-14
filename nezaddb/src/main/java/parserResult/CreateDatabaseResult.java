package parserResult;

import globalDefinition.CONSTANT;

public class CreateDatabaseResult extends ParseResult{
	private String databaseName;
	public CreateDatabaseResult(String databaseName){
		this.databaseName = databaseName;
		setSqlType(CONSTANT.SQL_CREATEDATABASE);	
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("createdatabase parse result:");
		System.out.println(databaseName);
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

}
