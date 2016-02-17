package parserResult;

import globalDefinition.CONSTANT;

import java.util.List;

public class ImportDataResult extends ParseResult{
	private List<?> fileList;

	public ImportDataResult(List fileList){
		setSqlType(CONSTANT.SQL_IMPORTDATA);	
		this.fileList = fileList;
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		
		System.out.println("ImportData parse result:");
		for(int i=0;i<fileList.size();++i){
			System.out.println(fileList.get(i).toString());
		}
	}

	public void setFileList(List fileList) {
		this.fileList = fileList;
	}

	public List getFileList() {
		return fileList;
	}

}
