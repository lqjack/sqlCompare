package net.sf.jsqlparser.statement.setsite;

import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class SetSite implements Statement{
	private List ipList;
	private List portList;
	private List siteNameList;
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		statementVisitor.visit(this);
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
	public void setSiteNameList(List siteNameList) {
		this.siteNameList = siteNameList;
	}
	public List getSiteNameList() {
		return siteNameList;
	}

}
