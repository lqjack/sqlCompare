package net.sf.jsqlparser.statement.fragment;

import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class VerticalFragment implements Statement {
	private Table table;
	private List subTableList;
	private List<List> columns; 
	
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		statementVisitor.visit(this);
	}

	public void setColumns(List<List> columns) {
		this.columns = columns;
	}

	public List<List> getColumns() {
		return columns;
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

}
