package configuration;

import static org.junit.Assert.assertNotNull;

import java.io.StringReader;
import java.sql.SQLException;

import org.junit.Test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

//@RunWith(PowerMockRunner.class)
public class ConfigurartionIT {

	@Test
	public void testCfg() throws SQLException, ReflectiveOperationException, JSQLParserException {
		Configuration config = Configuration.getInstance();
		assertNotNull(config);
		CCJSqlParserManager pm = new CCJSqlParserManager();
		Statement statment = pm.parse(new StringReader(config.getSrcSql()));
		assertNotNull(statment);
		
		
	}
}
