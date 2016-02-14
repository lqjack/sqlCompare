package slaveExecute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import slaveExecuteResult.DeleteBatchResult;
import slaveExecuteResult.GetAllTableResult;

import communication.ClientBase;

import dbcore.DbManager;
import dbcore.DbTable;

import executeResult.DeleteExecuteResult;
import gdd.ColumnInfo;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteInfo;
import gdd.TableInfo;
import globalDefinition.CONSTANT;
import globalDefinition.ExpressionJudger;
import globalDefinition.SimpleExpression;

public class DeleteRecordExecute {
	
	private GDD gdd;
	DeleteExecuteResult deleteResult;
	
	public DeleteRecordExecute(DeleteExecuteResult deleteResult){
		this.deleteResult = deleteResult;
		gdd = GDD.getInstance();
	}
	
	public int DeleteRecord() throws Exception {
		String tableName = deleteResult.getTableName();
		TableInfo tableinfo  = gdd.getTableInfo(tableName);
		
		if(tableinfo.getFragType() != CONSTANT.FRAG_VERTICAL)
			return -1;
		
		Vector<FragmentationInfo> fragmentations = tableinfo.getFragmentationInfo();
		if(fragmentations.size() <= 1 ){
			System.out.println("frag size error!");
			return -1;
		}
		int i,j;
		
		
		 DbManager dbm = new DbManager();
		 DbTable localTable = dbm.executeSelect("select * from "+fragmentations.elementAt(0).getFragName());
		
		
		
		Vector<DbTable> tables = new Vector<DbTable>();
		for(i = 1 ; i < fragmentations.size(); i++){
			FragmentationInfo fragInfo = fragmentations.elementAt(i);
			String siteName = fragInfo.getFragSiteName();
			SiteInfo siteinfo = gdd.getSiteInfo(siteName);
			String sql = "select * from " + fragInfo.getFragName();
			GetAllTableResult getAllTableResult = new GetAllTableResult(fragInfo.getFragName(),sql);
			ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
	        System.out.println("get all table client create success ");
			Object result = client.sendContext("getalltable", getAllTableResult);
			
			DbTable table = (DbTable)result;
			tables.add(table);
		}
		
		if(tables.size() <= 0){
			System.out.println("table num is error!");
			return -1;
		}
		
		Vector<Integer> ids = getDeleteID(localTable,tables);
		
		System.out.println("ids size="+ids.size());
		
		
		Vector<ColumnInfo> colInfos = gdd.getKeyColumns(deleteResult.getTableName());
		String keyColName = colInfos.elementAt(0).getColumnName();
		System.out.println("keyColName = " + keyColName);
		for(i = 1 ; i < fragmentations.size(); i++){
			FragmentationInfo fragInfo = fragmentations.elementAt(i);
			String siteName = fragInfo.getFragSiteName();
			SiteInfo siteinfo = gdd.getSiteInfo(siteName);
			
			DeleteBatchResult deleteBatchResult =  new DeleteBatchResult();
			String sql;
			for(j = 0 ; j < ids.size(); j++){
				sql = "delete from "+fragInfo.getFragName()+" where " + keyColName + " = "+ids.elementAt(j);
				System.out.println("delete sql="+sql);
				deleteBatchResult.addSql(sql);
			}
			
			ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
	        System.out.println("delete batch client create success ");
			Object result = client.sendContext("deletebatchtable", deleteBatchResult);
			
			Boolean re = (Boolean)result;
			if(re){
				System.out.println("delete success!");
			}
			else
				System.out.println("delete wrong!");
		}
		
		
		String tablename = fragmentations.elementAt(0).getFragName();
		DeleteBatchResult deleteBatchResult =  new DeleteBatchResult();
		String sql;
		for(j = 0 ; j < ids.size(); j++){
			sql = "delete from "+tablename+" where " + keyColName + " = "+ids.elementAt(j); 
			System.out.println("delete sql="+sql);
			deleteBatchResult.addSql(sql);
		}

		dbm.executeExecuteBatch(deleteBatchResult.getSqls());
		
		return ids.size();
	}
	
	public Vector<Integer> getDeleteID(DbTable localTable, Vector<DbTable> tables){
		Vector<Integer> ids = new Vector<Integer>();
		
		System.out.println("localTable size="+localTable.getRowCount());
		
		for(int i = 0 ; i < tables.size() ; i++){
			System.out.println("tables "+ i +" size="+tables.elementAt(i).getRowCount());
			if(tables.elementAt(i).getRowCount() != localTable.getRowCount()){
				System.out.println("table size is not right!");
				return null;
			}
		}
		
		
		tables.add(localTable);
		
		
		Vector<SimpleExpression> expressions = new Vector<SimpleExpression>();
		for(int i = 0 ; i < deleteResult.getExpressions().size(); i++){
			expressions.add(deleteResult.getExpressions().get(i));
			deleteResult.getExpressions().get(i).displayResult();
		}
		
		
		Vector<ColumnInfo> colInfos = gdd.getKeyColumns(deleteResult.getTableName());
		String keyColName = colInfos.elementAt(0).getColumnName();
		System.out.println("keyColName = " + keyColName);
		
		
		Map<String, String> m = new HashMap<String, String>();
		System.out.println("localTable size="+localTable.getRowCount());
		System.out.println("tables.size="+tables.size());
		for(int i = 0 ; i < localTable.getRowCount() ; i ++){
			m.clear();
			for(int j = 0 ; j < tables.size() ; j++){
				ArrayList<String> colName = tables.elementAt(j).getColName();
				for(int k = 0 ; k < colName.size(); k++)
					m.put(colName.get(k),tables.elementAt(j).getRows().get(i).get(k).toString());
			}
			
			if(ExpressionJudger.judgeRecord(expressions, m)){
				ids.add((Integer.parseInt(m.get(keyColName))));
			}
		}
		
		
		
		return ids;
	}
	
	
	
	
	
	
	
	
	
	
}
