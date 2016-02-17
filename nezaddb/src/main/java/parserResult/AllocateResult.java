package parserResult;

import java.util.ArrayList;
import java.util.List;

import globalDefinition.CONSTANT;

public class AllocateResult extends ParseResult{
	public class SiteTableMap{
		public String siteName;
		public List<?> tables;
	}
	private List<?> siteList;
	private List<List> tableList;
	private List<SiteTableMap> siteTableMap;
	public AllocateResult(List<?> siteList,List<List> tableList){
		setSqlType(CONSTANT.SQL_ALLOCATE);
		this.siteList = siteList;
		this.tableList = tableList;
		genSiteTableMap();
	}
	public void setSiteList(List<?> siteList) {
		this.siteList = siteList;
	}
	public List<?> getSiteList() {
		return siteList;
	}
	public void setTableList(List<List> tableList) {
		this.tableList = tableList;
	}
	public List<List> getTableList() {
		return tableList;
	}
	public void setSiteTableMap(List<SiteTableMap> siteTableMap) {
		this.siteTableMap = siteTableMap;
	}
	public List<SiteTableMap> getSiteTableMap() {
		return siteTableMap;
	}
	private void genSiteTableMap(){
		if(siteList.size()==0) return;
		siteTableMap =  new ArrayList<SiteTableMap>();
		for(int i=0;i<siteList.size();++i){
			SiteTableMap st = new SiteTableMap();
			st.siteName = siteList.get(i).toString();
			st.tables = tableList.get(i);
			siteTableMap.add(st);
		}
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		visitor.visit(this);
	}
	@Override
	public void displayResult() {
		System.out.println("Allocate parse result:");
		for(int i=0;i<siteList.size();++i){
			System.out.println(siteList.get(i).toString());
		}
		for(int i=0;i<tableList.size();++i){
			for(int j=0;j<tableList.get(i).size();++j){
				System.out.print(tableList.get(i).get(j).toString()+", ");
			}
			System.out.println();
		}
	}
}
