package execute;

import parserResult.ParseResult;

public abstract class ExecuteSQL {
	private int sqlType;
	
	public abstract void execute(ParseResult result);
	
	public int getSqlType() {
		return sqlType;
	}

	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}
}
