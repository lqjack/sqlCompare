package parserResult;

import java.util.ArrayList;
import java.util.List;

import globalDefinition.CONSTANT;

public class VFragmentResult extends ParseResult{
	public class VFragmentUnit{
		public String tableName;
		public List<String> col;
	}
	
	private String tableName;
	private List<?> subTableList;
	private List<List> columns;
	private List<VFragmentUnit> vFragmentMap;
	public List<VFragmentUnit> getVFragmentMap(){
		return vFragmentMap;
	}
	public VFragmentResult(String tableName,List subTableList,List<List>columns){
		setSqlType(CONSTANT.SQL_V_FRAGMENT);
		this.tableName = tableName;
		this.subTableList = subTableList;
		this.columns = columns;
		genVFragmentMap();
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setSubTableList(List subTableList) {
		this.subTableList = subTableList;
	}
	public List getSubTableList() {
		return subTableList;
	}
	public void setColumns(List<List> columns) {
		this.columns = columns;
	}
	public List<List> getColumns() {
		return columns;
	}
	private void genVFragmentMap(){
		if(subTableList.size()==0) return;
		vFragmentMap = new ArrayList<VFragmentUnit>();
		for(int i=0;i<subTableList.size();++i){
			VFragmentUnit v = new VFragmentUnit();
			v.tableName = subTableList.get(i).toString();
			v.col =  new ArrayList<String>();
			for(int j=0;j<columns.get(i).size();++j){
				v.col.add(columns.get(i).get(j).toString());
			}
			vFragmentMap.add(v);
		}
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("Vertical fragment parse result:");
		System.out.println(tableName);
		for(int i=0;i<subTableList.size();++i){
			System.out.println(subTableList.get(i).toString());
		}
		for(int i=0;i<columns.size();++i){
			for(int j=0;j<columns.get(i).size();++j){
				System.out.print(columns.get(i).get(j).toString()+", ");
			}
			System.out.println();
		}
	}
}
