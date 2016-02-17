package net.sf.jsqlparser;

import java.io.StringReader;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import org.junit.Assert;
import org.junit.Test;

import sqlParser.SqlParser;

public class SelectParserIT {

	@Test
	public void test() {
		Assert.assertTrue(true);
	}

	@Test
	public void testSelect() {
		String sql = "SELECT * FROM T";

		CCJSqlParserManager sqlParserManger = new CCJSqlParserManager();
		try {
			Statement statement = sqlParserManger.parse(new StringReader(sql));
			statement.accept(new SqlParser(sql, true));
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
	}
}
