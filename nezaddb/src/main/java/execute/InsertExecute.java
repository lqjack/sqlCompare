package execute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import communication.ClientBase;

import executeResult.InsertExecuteResult;
import executeReturnResult.ExecuteReturnResult;
import executeReturnResult.InsertReturnResult;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteInfo;
import gdd.TableInfo;
import globalDefinition.CONSTANT;
import globalDefinition.ExpressionJudger;
import globalDefinition.SimpleExpression;
import parserResult.InsertResult;
import parserResult.ParseResult;
import queryTree.*;
import slaveExecuteResult.SlaveInsertResult;

public class InsertExecute extends ExecuteSQL{
	
	
	private GDD gdd;
	private Vector<InsertExecuteResult> insertResults;
	private InsertReturnResult insertReturnResult;
	
	public InsertExecute(){
		gdd = GDD.getInstance();
		this.insertResults = new Vector<InsertExecuteResult>();
	}
	
	public ExecuteReturnResult getResult(){
		return this.insertReturnResult;
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		InsertResult insertResult = (InsertResult)result;
		QueryTree insertTree = insertResult.getInsertTree();
		
		System.out.println("insert info:");
		insertTree.displayTree();
		
		if(genInsertResult(insertTree)){
			this.displayResult();
			
			try{
				allocateTaskSites(); //allocate the task to all sites
			}catch(IOException ex){
				System.out.println(ex.toString());
				ex.printStackTrace();
			}	
		}	
	}
	
	public boolean updateGDDInfo(){
		return true;
	}
	
	public boolean allocateTaskSites()throws IOException{
		
		//boolean flag = true;
		
		if(this.insertResults == null)
			return false;
		
		this.insertReturnResult = new InsertReturnResult();
		//for(int i = 0 ; i < 1 ; i++){
		for(int i = 0 ; i < this.insertResults.size() ; i++){
			SiteInfo siteinfo = gdd.getSiteInfo(this.insertResults.elementAt(i).getSiteName());
			ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
			System.out.println("client create success");
			Object result = client.sendContext("inserttable", this.insertResults.elementAt(i));	
			System.out.println("site1 over!");
			if(!(result instanceof SlaveInsertResult)){
				System.out.println("error object");
			}
			else{
				SlaveInsertResult slaveInsertResult = (SlaveInsertResult)result;
				String s;
				
				//if insert success, update the gdd info
				if(slaveInsertResult.getSuccess()){
					s = "insert record into site" + siteinfo.getSiteID() + " succeed!";
					FragmentationInfo fragInfo = gdd.getFragmentation(this.insertResults.elementAt(i).getTableName());
					fragInfo.setFragSize(fragInfo.getFragSize()+1);
				}
				else
					s = "insert record into site" + siteinfo.getSiteID() + "failed! " + slaveInsertResult.getErrorInfo();
				this.insertReturnResult.addinfo(s);		
			}
		}
		this.insertReturnResult.displayResult();
		
		return true;
	}
	
	public boolean genInsertResult(QueryTree insertTree){
		
		SelectionNode root = (SelectionNode)insertTree.getRoot();
		String tableName;
		
		System.out.println("tableName = "+root.getTableName());
		
		tableName = root.getCondList().get(0).tableName;
		
		System.out.println("tablename="+tableName);
		TableInfo tableinfo = gdd.getTableInfo(tableName);
		int fragType = tableinfo.getFragType();
		
		switch(fragType){
			case CONSTANT.FRAG_HORIZONTAL:
				return genInsertResultHorizontal(insertTree,root);
			case CONSTANT.FRAG_VERTICAL:
				return genInsertResultVertical(insertTree,root);
			case CONSTANT.FRAG_HYBIRD:
				return genInsertResultHybird(insertTree,root);
			default:
				return false;
		}	
	}
	
	public boolean genInsertResultHorizontal(QueryTree insertTree,SelectionNode root){
		
		ArrayList<TreeNode> childList = root.getChildList();
		
		if(childList == null)
			return false;
		Map<String, String> m = new HashMap<String, String>();
		List<SimpleExpression> condList = root.getCondList();
		String tableName = root.getTableName();
		String insertSql = null;
		String colName = "(";
		String value = " values(";
		for(int i = 0 ; i < condList.size() ; i++){
			colName += condList.get(i).columnName;
			if(condList.get(i).valueType == CONSTANT.VALUE_INT){
				value += condList.get(i).value;
				m.put(condList.get(i).columnName, condList.get(i).value);
			}
			else{
				value += "'" + condList.get(i).value + "'";
				m.put(condList.get(i).columnName, "'"+ condList.get(i).value + "'");
			}
			if(i < (condList.size() - 1)){
				colName += ",";
				value += ",";
			}
		}
		colName += ")";
		value += ")";
		
		
		TreeNode nextNode = childList.get(0);
		if(nextNode instanceof UnionNode){                          //if no optimization
			ArrayList<TreeNode> children = nextNode.getChildList();
			for(int i = 0 ; i < children.size() ; i++){
				TreeNode node = children.get(i);
				tableName = node.getContent();
				FragmentationInfo fragInfo = gdd.getFragmentation(tableName);
				if(fragInfo == null){
					System.out.println(tableName + " is not exist");
					return false;
				}
				Vector<SimpleExpression> expressions = fragInfo.getFragConditionExpression().HorizontalFragmentationCondition;
				
				if(ExpressionJudger.judgeRecord(expressions, m)){
					insertSql = "insert into " + tableName + colName + value;				
					String sitename = "site" + node.getSiteID();
					InsertExecuteResult insertResult = new InsertExecuteResult(sitename,tableName,insertSql);
					this.insertResults.add(insertResult);
				}
			}
		}
		else{                             //optimization
			if(childList.size() != 1){
				return false;
			}
			tableName = nextNode.getContent();
			insertSql = "insert into " + tableName + colName + value;
			System.out.println(insertSql);
			
			String sitename = "site" + nextNode.getSiteID();
			InsertExecuteResult insertResult = new InsertExecuteResult(sitename,tableName,insertSql);
			this.insertResults.add(insertResult);
		}
		return true;
	}
	
	public boolean genInsertResultVertical(QueryTree insertTree,SelectionNode root){
		
		List<SimpleExpression> condList = root.getCondList();

		ArrayList<TreeNode> childList = root.getChildList();
		TreeNode nextnode = childList.get(0);
		
		ArrayList<TreeNode> children;
		if(nextnode instanceof JoinNode)
			children = nextnode.getChildList();
		else
			children = root.getChildList();
		
		TreeNode node;
		String colName = null;
		String value = null;
		for(int i = 0 ; i < children.size() ; i++){
			node = children.get(i);
			
			if(!(node instanceof LeafNode))
				return false;
			
			String tableName = node.getContent();
			FragmentationInfo fragInfo = gdd.getFragmentation(tableName);
			Vector<String> colNames = fragInfo.getFragConditionExpression().verticalFragmentationCondition;
			
			colName =  "(";
			value = " values(";		
			for(int j = 0 ; j < condList.size() ; j++){
				
				boolean flag = false;
				int k;
				for(k = 0 ; k < colNames.size(); k++){
					if(colNames.elementAt(k).equals(condList.get(j).columnName)){
						flag = true;
						break;
					}
				}
				if(flag){
					colName += condList.get(j).columnName;
					if(condList.get(j).valueType == CONSTANT.VALUE_INT){
						value += condList.get(j).value;
					}
					else{
						value += "'" + condList.get(j).value + "'";
					}
					if(k < (colNames.size() - 1)){
						colName += ",";
						value += ",";
					}
				}
			}
			colName += ")";
			value += ");";		
			
			String sitename = "site" + node.getSiteID();
			String insertSql = "insert into " + tableName + colName + value;
			InsertExecuteResult insertResult = new InsertExecuteResult(sitename,tableName,insertSql);
			this.insertResults.add(insertResult);	
		}
		return true;
	}
	
	public boolean genInsertResultHybird(QueryTree insertTree,SelectionNode root){
		
		return true;
	}
	
	public void displayResult(){
		System.out.println("insert Result info:");
		for(int i = 0 ; i  < this.insertResults.size() ; i++)
			this.insertResults.elementAt(i).displayResult();
	}
}
