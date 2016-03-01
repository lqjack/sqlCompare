package net.sf.jsqlparser;

import java.io.StringReader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

@RunWith(PowerMockRunner.class)
public class SqlParserIT {

	String sql = "select * from A ";

	@Test
	public void parseSql() throws ParseException, JSQLParserException {
		CCJSqlParserManager pm = new CCJSqlParserManager();
//		CCJSqlParser parser = pm.getInstance(sql);
//		Select select = parser.select();
//		Assert.assertNotNull(select);
		
		Statement statment = pm.parse(new StringReader(sql));
		StatementDeParser sdParser = new StatementDeParser(statment);
		sdParser.visit();
	}
}
