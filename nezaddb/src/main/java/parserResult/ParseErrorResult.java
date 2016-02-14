package parserResult;

import globalDefinition.CONSTANT;

public class ParseErrorResult extends ParseResult{
	private String errorInfo;
	
	public ParseErrorResult(String info){
		setSqlType(CONSTANT.SQL_PARSER_ERROR);
		errorInfo = info;
	}
	public void setErrorInfo(String errorInfo) {
		this.errorInfo = errorInfo;
	}
	public String getErrorInfo() {
		return errorInfo;
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println(errorInfo);
	}

}
