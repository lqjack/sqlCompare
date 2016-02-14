package parserResult;

import java.util.ArrayList;
import java.util.List;

import globalDefinition.CONSTANT;

public class SetSiteResult extends ParseResult{
	public class SiteIpMap{
		public String siteName;
		public String siteIP;
		public int sitePort;
	}
	
	private List<?> siteNameList;
	private List<?> ipList;
	private List<?> portList;
	private List<SiteIpMap> siteIpMap;

	public SetSiteResult(List siteNameList,List ipList,List portList){
		setSqlType(CONSTANT.SQL_SETSITE);
		this.siteNameList = siteNameList;
		this.ipList = ipList;
		this.portList = portList;
		genSiteIpMap();
	}
	public void setSiteNameList(List siteNameList) {
		this.siteNameList = siteNameList;
	}
	public List getSiteNameList() {
		return siteNameList;
	}
	public void setIpList(List ipList) {
		this.ipList = ipList;
	}
	public List getIpList() {
		return ipList;
	}
	public void setPortList(List portList) {
		this.portList = portList;
	}
	public List getPortList() {
		return portList;
	}
	private void genSiteIpMap(){
		if(ipList.size()==0) return;
		siteIpMap = new ArrayList<SiteIpMap>();
		for(int i=0;i<ipList.size();++i){
			SiteIpMap st = new SiteIpMap();
			st.siteIP = ipList.get(i).toString();
			st.siteName = siteNameList.get(i).toString();
			st.sitePort = Integer.parseInt(portList.get(i).toString());
			siteIpMap.add(st);
		}
	}
	
	public void setSiteIpMap(List<SiteIpMap> siteIpMap) {
		this.siteIpMap = siteIpMap;
	}
	public List<SiteIpMap> getSiteIpMap() {
		return siteIpMap;
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("Setsite parse result:");
		for(int i=0;i<siteNameList.size();++i){
			System.out.println(siteNameList.get(i).toString());
		}
		for(int i=0;i<ipList.size();++i){
			System.out.println(ipList.get(i).toString());
		}
		for(int i=0;i<portList.size();++i){
			System.out.println(portList.get(i).toString());
		}
	}
}
