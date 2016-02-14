package net.sf.jsqlparser.statement.fragment;

import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class HorizontalFragment implements Statement{
	private Table table;
	private List subTableList;
	private List<List> conditions;
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		statementVisitor.visit(this);
	}
	public void setTable(Table table) {
		this.table = table;
	}
	public Table getTable() {
		return table;
	}
	public void setSubTableList(List subTableList) {
		this.subTableList = subTableList;
	}
	public List getSubTableList() {
		return subTableList;
	}
	public void setConditions(List<List> conditions) {
		this.conditions = conditions;
	}
	public List<List> getConditions() {
		return conditions;
	}

}
