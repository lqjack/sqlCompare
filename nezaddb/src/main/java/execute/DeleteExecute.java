package execute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import communication.ClientBase;

import executeResult.DeleteExecuteResult;
import executeReturnResult.DeleteReturnResult;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteInfo;
import gdd.TableInfo;
import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;
import parserResult.DeleteResult;
import parserResult.ParseResult;
import queryTree.JoinNode;
import queryTree.LeafNode;
import queryTree.QueryTree;
import queryTree.SelectionNode;
import queryTree.TreeNode;
import queryTree.UnionNode;
import slaveExecuteResult.SlaveDeleteResult;

public class DeleteExecute extends ExecuteSQL{
	
	
	private GDD gdd;
	private Vector<DeleteExecuteResult> deleteResults;
	private DeleteReturnResult deleteReturnResult;
	
	
	public DeleteExecute(){
		gdd = GDD.getInstance();
		this.deleteResults = new Vector<DeleteExecuteResult>();
		
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		DeleteResult deleteResult = (DeleteResult)result;
		QueryTree deleteTree = deleteResult.getDeleteTree();
		System.out.println("delete tree ifno:");
		deleteTree.displayTree();
		
		
		if(genDeleteResult(deleteTree)){
			this.displayResult();
			
			
			try{
				allocateTaskSites();
			}catch(IOException ex){
				System.out.println(ex.toString());
				ex.printStackTrace();
			}
			
			
		}
		
	}
	
	public boolean allocateTaskSites()throws IOException{
		//boolean flag = true;
		
		if(this.deleteResults == null)
			return false;
		
		this.deleteReturnResult = new DeleteReturnResult();
		//for(int i = 0 ; i < 1 ; i++){
		for(int i = 0 ; i < this.deleteResults.size() ; i++){
			SiteInfo siteinfo = gdd.getSiteInfo(this.deleteResults.elementAt(i).getSiteName());
			ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
			System.out.println("client create success");
			Object result = client.sendContext("deletetable", this.deleteResults.elementAt(i));	
			System.out.println("site1 over!");
			
			
			if(!(result instanceof SlaveDeleteResult)){
				System.out.println("error object");
				return false;
			}
			else{
				SlaveDeleteResult slaveDeleteResult = (SlaveDeleteResult)result;
				String s;
				
				if(slaveDeleteResult.getSuccess()){
					s = "delete record on site" + siteinfo.getSiteID() + " succeed!";
		    		switch(this.deleteResults.elementAt(i).getFragType()){
						case CONSTANT.FRAG_HORIZONTAL:
							FragmentationInfo fragInfo = gdd.getFragmentation(this.deleteResults.elementAt(i).getTableName());
							fragInfo.setFragSize(fragInfo.getFragSize() - slaveDeleteResult.getAffectedNum());
							break;
						case CONSTANT.FRAG_VERTICAL:
							String tablename = this.deleteResults.elementAt(i).getTableName();
							TableInfo  tableinfo = gdd.getTableInfo(tablename);
							Vector<FragmentationInfo> fraginfos = tableinfo.getFragmentationInfo();
							for(int k = 0 ; k < fraginfos.size() ; k++){
								fraginfos.elementAt(i).setFragSize(fraginfos.elementAt(i).getFragSize() - slaveDeleteResult.getAffectedNum());
							}
							break;
						case CONSTANT.FRAG_HYBIRD:
							break;
						default:
		    		}	
				}
				else
					s = "delete record on site" + siteinfo.getSiteID() + "failed! " + slaveDeleteResult.getErrorInfo();
				this.deleteReturnResult.addinfo(s);		
			}
		}
		this.deleteReturnResult.displayResult();
		return true;	
	}
	
	public boolean genDeleteResult(QueryTree deleteTree){
		
		SelectionNode root = (SelectionNode)deleteTree.getRoot();
		String tableName;
		
		
		tableName = root.getCondList().get(0).tableName;
		
		System.out.println("tablename="+tableName);
		TableInfo tableinfo = gdd.getTableInfo(tableName);
		int fragType = tableinfo.getFragType();
		
		switch(fragType){
			case CONSTANT.FRAG_HORIZONTAL:
				return genDeleteResultHorizontal(deleteTree,root);
			case CONSTANT.FRAG_VERTICAL:
				return genDeleteResultVertical(deleteTree,root);
			case CONSTANT.FRAG_HYBIRD:
				return genDeleteResultHybird(deleteTree,root);
			default:
				return false;
		}	
	}
	
	public boolean genDeleteResultHorizontal(QueryTree deleteTree,SelectionNode root){
		
		ArrayList<TreeNode> childList = root.getChildList();
		
		if(childList == null)
			return false;
		
		//Map m = new HashMap();
		List<SimpleExpression> condList = root.getCondList();
		//String tableName = root.getTableName();
		String conditionString ="";
		
		for(int i = 0 ; i < condList.size() ; i++){
			conditionString += condList.get(i).columnName +" "+condList.get(i).op+" ";
			if(condList.get(i).valueType == CONSTANT.VALUE_INT){
				conditionString += condList.get(i).value;
			}
			else{
				conditionString += "'" + condList.get(i).value + "'";
			}
			if(i < (condList.size() - 1)){
				conditionString += " and ";
			}
		}
		System.out.println(conditionString);
		
		TreeNode nextnode = childList.get(0);
		
		ArrayList<TreeNode> children;
		if(nextnode instanceof UnionNode)
			children = nextnode.getChildList();
		else
			children = root.getChildList();
		
		
		TreeNode node;
		for(int i = 0 ; i < children.size() ; i++){
			node = children.get(i);
			if(!(node instanceof LeafNode))
				return false;
			String tablename = node.getContent();
			String sitename = "site" + node.getSiteID();
			String deleteSql = "delete from "+tablename+" where "+conditionString;
			DeleteExecuteResult deleteResult = new DeleteExecuteResult(sitename,tablename,CONSTANT.FRAG_HORIZONTAL,deleteSql,null);
			this.deleteResults.add(deleteResult);
		}
		return true;
	}
	
	public boolean genDeleteResultVertical(QueryTree insertTree,SelectionNode root){
		
		ArrayList<TreeNode> childList = root.getChildList();
		
		if(childList == null)
			return false;
		
		List<SimpleExpression> condList = root.getCondList();
		TreeNode nextnode = childList.get(0);
		
		ArrayList<TreeNode> children;
		if(nextnode instanceof JoinNode)
			children = nextnode.getChildList();
		else
			children = root.getChildList();
		
		if(children == null || children.size() == 0)
			return false;
		
		TreeNode node = children.get(0);
		String tablename = root.getCondList().get(0).tableName;
		String sitename = "site" + node.getSiteID();
		//String deleteSql = null;
		DeleteExecuteResult deleteResult = new DeleteExecuteResult(sitename,tablename,CONSTANT.FRAG_VERTICAL,null,condList);
		this.deleteResults.add(deleteResult);
		return true;
	}
	
	public boolean genDeleteResultHybird(QueryTree insertTree,SelectionNode root){
		return true;
	}
	
	public void displayResult(){
		System.out.println("delete Result info:");
		for(int i = 0 ; i  < this.deleteResults.size() ; i++)
			this.deleteResults.elementAt(i).displayResult();
	}
	

}
