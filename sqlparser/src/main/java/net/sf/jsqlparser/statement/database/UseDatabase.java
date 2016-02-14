package net.sf.jsqlparser.statement.database;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class UseDatabase implements Statement{
	private String databaseName;
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		statementVisitor.visit(this);
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getDatabaseName() {
		return databaseName;
	}

}
