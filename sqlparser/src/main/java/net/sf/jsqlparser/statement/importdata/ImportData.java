package net.sf.jsqlparser.statement.importdata;

import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class ImportData implements Statement{
	private List fileList;
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		statementVisitor.visit(this);
	}
	public void setFileList(List fileList) {
		this.fileList = fileList;
	}
	public List getFileList() {
		return fileList;
	}

}
