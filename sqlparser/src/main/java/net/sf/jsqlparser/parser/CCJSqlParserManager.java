package net.sf.jsqlparser.parser;

import java.io.Reader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;

public class CCJSqlParserManager implements JSqlParser {
	
	public Statement parse(Reader statementReader) throws JSQLParserException {
		CCJSqlParser parser = new CCJSqlParser(statementReader);
		try {
			return parser.statement();
		} catch (Throwable e) {
			throw new JSQLParserException(e);
		}
	}
	
	public CCJSqlParser getInstance(String sql){
		return new  CCJSqlParser(sql);
	}
}
