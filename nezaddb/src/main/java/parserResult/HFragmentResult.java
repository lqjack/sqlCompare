package parserResult;

import java.util.ArrayList;
import java.util.List;

import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

public class HFragmentResult extends ParseResult{
	
	public class HFragmentUnit{
		private String tableName;
		private List<SimpleExpression> cond;
		public void setTableName(String tableName) {
			this.tableName = tableName;
		}
		public String getTableName() {
			return tableName;
		}
		public List<SimpleExpression> getExpression(){
			return cond;
		}
		public HFragmentUnit(){
			cond = new ArrayList<SimpleExpression>();
		}
	}
	private String tableName;
	private List<?> subTableList;
	private List<List> conditions;
	private List<HFragmentUnit> hFragmentMap;
	public List<HFragmentUnit> getHFragmentMap(){
		return hFragmentMap;
	}
	public HFragmentResult(String tableName,List subTableList, List<List>conditions){
		setSqlType(CONSTANT.SQL_H_FRAGMENT);
		this.tableName = tableName;
		this.subTableList = subTableList;
		this.conditions = conditions;
		genHFragmentMap();
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
	private void genHFragmentMap(){
		if(subTableList.size()==0) return;
		hFragmentMap =  new ArrayList<HFragmentUnit>();
		for(int i=0;i<subTableList.size();++i){
			HFragmentUnit u = new HFragmentUnit();
			u.setTableName(subTableList.get(i).toString());
			u.cond = new ArrayList<SimpleExpression>();
			for(int j=0;j<conditions.get(i).size();++j){
				SimpleExpression simpleExp = new SimpleExpression();
				BinaryExpression exp = (BinaryExpression) conditions.get(i).get(j);
				simpleExp.op = exp.getStringExpression().toString();
				simpleExp.value = exp.getRightExpression().toString();
				simpleExp.columnName = exp.getLeftExpression().toString();
				simpleExp.tableName = u.getTableName();
				if(exp.getRightExpression() instanceof LongValue){
					simpleExp.valueType = CONSTANT.VALUE_INT;
				}
				else if(exp.getRightExpression() instanceof StringValue){
					simpleExp.valueType = CONSTANT.VALUE_STRING;
				}
				else if(exp.getRightExpression() instanceof DoubleValue){
					simpleExp.valueType = CONSTANT.VALUE_DOUBLE;
				}
				u.cond.add(simpleExp);
			}
			hFragmentMap.add(u);
		}
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableName() {
		return tableName;
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("Horizontal fragment parse result:");
		System.out.println(tableName);
		for(int i=0;i<subTableList.size();++i){
			System.out.println(subTableList.get(i).toString());
		}
		for(int i=0;i<conditions.size();++i){
			for(int j=0;j<conditions.get(i).size();++j){
				BinaryExpression exp = (BinaryExpression) conditions.get(i).get(j);
				System.out.print(exp.getLeftExpression().toString()+" ");
				System.out.print(exp.getStringExpression());
				System.out.print(exp.getRightExpression().toString()+", ");
			}
			System.out.println();
		}
	}
}
