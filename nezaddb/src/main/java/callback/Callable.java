package callback;

import java.sql.ResultSet;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;

public interface Callable {

	void resolveExpression(Statement statement);
	
	void resolveExpression(Expression expression);
	
	void test(ResultSet resultSet);
}
