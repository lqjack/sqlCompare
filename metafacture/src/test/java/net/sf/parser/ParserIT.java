package net.sf.parser;

import net.sf.jsqlparser.JSQLParserException;

import org.junit.Test;

import sqlParser.SqlParser;

public class ParserIT {

	@Test
	public void test() throws JSQLParserException {
		String sql = "SELECT * FROM　USER";
		SqlParser sqlParser = new SqlParser(sql, true);
		sqlParser.getResult().displayResult();
	}
}
