package net.sf.jsqlparser.statement.allocate;

import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class Allocate implements Statement{
	private List<List> tableList;
	private List<?> siteList;
	@Override
	public void accept(StatementVisitor statementVisitor) {
		statementVisitor.visit(this);
	}
	public void setTableList(List<List> tableList) {
		this.tableList = tableList;
	}
	public List<List> getTableList() {
		return tableList;
	}
	public void setSiteList(List siteList) {
		this.siteList = siteList;
	}
	public List getSiteList() {
		return siteList;
	}

}
