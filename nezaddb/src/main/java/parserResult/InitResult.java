package parserResult;

import globalDefinition.CONSTANT;

public class InitResult extends ParseResult{
	private String fileName;
	public InitResult(String fileName){
		this.fileName = fileName;
		setSqlType(CONSTANT.SQL_INIT);	
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("init parse result:");
		System.out.println(fileName);
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

}
