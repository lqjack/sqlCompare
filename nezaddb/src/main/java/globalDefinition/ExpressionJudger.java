package globalDefinition;

import java.util.Map;
import java.util.Vector;

public class ExpressionJudger {
		
	public static boolean judgeRecord(Vector<SimpleExpression>expressions,Map<String,String> map){
		
		if(map == null)
			return false;
		
		String value;
		for(int i = 0 ; i < expressions.size() ; i++){
			SimpleExpression expression = expressions.elementAt(i);
			value = expression.value;
			if(!(map.containsKey(expression.columnName) && judgeValue(expression.op,map.get(expression.columnName).toString(),value))){
				return false;
			}
		}
		return true;
	}
	
	public static boolean judgeValue(String op,String value1,String value2){
		int v1,v2;
		if(op.equals("=")){
			return value1.equals(value2);
		}else if(op.equals(">")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 > v2);
		}else if(op.equals("<")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 < v2);
		}else if(op.equals(">=")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 >= v2);
		}else if(op.equals("<=")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 <= v2);
		}else if(op.equals("==")){
			return !value1.equals(value2);
		}else{
			System.out.println("Error: error op!");
			return false;
		}
	}
	
}
