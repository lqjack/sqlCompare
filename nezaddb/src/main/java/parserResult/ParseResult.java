package parserResult;

public abstract class ParseResult {
	private int sqlType;
	public abstract void accept(ParseResultVisitor visitor);
	public abstract void displayResult();
	public int getSqlType() {
		return sqlType;
	}

	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}
	
}
