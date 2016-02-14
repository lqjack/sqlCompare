package queryTree;

import java.util.ArrayList;
import java.util.List;

import gdd.GDD;
import globalDefinition.CONSTANT;
import globalDefinition.JoinExpression;
import globalDefinition.SimpleExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import sqlParser.SelectItemsFinder;
import sqlParser.TableNamesFinder;
import sqlParser.WhereItemsFinder;

public class QueryTree {
	private TreeNode root = null;
	private List<FormattedTreeNode> nodeList = null;
	private List<LeafNode> leafNodeList = null;
	private int nodeID;
	private int treeType;
	private int classID;
	private String sql;
	public QueryTree(){
		setNodeID(0);
		setTreeType(0);
		setClassID(0);
	}
	public TreeNode getRoot(){
		return root;
	}
	public void setRoot(TreeNode r){
		root = r;
	}
	public boolean isValidTree(){
		return !(root == null);
	}
	public void displayTree(){
		recDisplayTree(root,0,0);
	}
	private void recDisplayTree(TreeNode thisNode, int level, int childNumber){
		System.out.print("level="+level+" child="+childNumber+": ");
		System.out.print(thisNode.getContent());
		System.out.println(" siteID="+thisNode.getSiteID()+" nodeID="+thisNode.getNodeID()+" classId="+thisNode.getClassID());
		int childNum = thisNode.getChildCount();
		for(int i=0; i<childNum; ++i){
			TreeNode nextNode = thisNode.getChildList().get(i);
			if(nextNode != null){
				recDisplayTree(nextNode,level+1,i);
			}
			else
				return;
		}
	}
	public int getClassficationSize(){
		return this.classID;
	}
	public void setNodeList(List<FormattedTreeNode> nodeList) {
		this.nodeList = nodeList;
	}
	public List<FormattedTreeNode> getNodeList() {
		return nodeList;
	}
	public void genTreeList(){
		if (root == null) return;
		nodeList = new ArrayList<FormattedTreeNode>();
		genTreeListByNode(root);
	}
	private void genTreeListByNode(TreeNode node){
		FormattedTreeNode n = new FormattedTreeNode();
		n.content = node.getContent();
		n.nodeID = node.getNodeID();
		n.parentNodeID = ((node.getParent() == null)?-1:node.getParent().getNodeID());
		n.siteID = node.getSiteID();
		nodeList.add(n);
		if (node.isLeaf())
			return;
		for(int i=0;i<node.getChildCount();++i){
			genTreeListByNode(node.getChild(i));
		}
	}
	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}
	public int getNodeID() {
		return nodeID;
	}
	public void setTreeType(int treeType) {
		this.treeType = treeType;
	}
	public int getTreeType() {
		return treeType;
	}
	public void genSelectTree(Select select){
		this.treeType = CONSTANT.TREE_SELECT;
		
		/*------from clause----*/
		ArrayList<LeafNode> leafs = new ArrayList<LeafNode>();
		TableNamesFinder finder = new TableNamesFinder();
		ArrayList<String> tableList = (ArrayList<String>) finder.getTableList(select);
		for (int i=0;i<tableList.size();++i){
			LeafNode node1 = new LeafNode();
			node1.setNodeName(tableList.get(i));
			node1.setTableName(tableList.get(i));
			node1.setSegment(false);
			node1.setNodeID(this.nodeID);
			this.nodeID++;
			leafs.add(node1);
		}
		
		
		/*------select clause----*/
		SelectItemsFinder finder2 = new SelectItemsFinder();
		ArrayList<String> projectionItemsList = finder2.getSelectItemsList(select);
		String attributes = new String();
		TreeNode curNode = null;
		for (int i=0;i<projectionItemsList.size();i++){
			ProjectionNode node = new ProjectionNode();
			node.setNodeName("Projection");
			attributes += projectionItemsList.get(i)+((i<projectionItemsList.size())?",":"");
			int pos = projectionItemsList.get(i).indexOf(".");
			if(pos == -1){
				if(tableList.size() == 1)
					node.addTableName(tableList.get(0));
				else
					node.addTableName("null");
				node.addAttribute(projectionItemsList.get(i));
			}
			else{
				String tableName = projectionItemsList.get(i).substring(0,pos);
				String attrName = projectionItemsList.get(i).substring(pos+1);
				node.addTableName(tableName);
				node.addAttribute(attrName);
			}
			if(i==0){
				node.setRoot();
				node.setParent(null);
				node.setNodeID(this.nodeID);
				this.nodeID++;
				root = node;
				curNode = root;
			}
			else{
				node.setParent(curNode);
				node.setNodeID(this.nodeID);
				this.nodeID++;
				curNode = node;
			}
			
		}
		
		
		
		/*------where clause----*/
		WhereItemsFinder finder3 = new WhereItemsFinder(select);
		/*------join clause----*/
		ArrayList<JoinNode> joins = new ArrayList<JoinNode>();
		ArrayList<JoinExpression> joinList = finder3.getJionList();
		for(int i=0;i<joinList.size();++i){
			JoinNode node2 = new JoinNode();
			node2.setLeftTableName(joinList.get(i).leftTableName);
			node2.setRightTableName(joinList.get(i).rightTableName);
			node2.addAttribute(joinList.get(i).leftColumn, joinList.get(i).rigthColumn);
			node2.setNodeName("Join");
			node2.setNodeID(this.nodeID);
			this.nodeID++;
			joins.add(node2);
			
		}
		
		/*-------selection clause-------*/	
		
		ArrayList<SelectionNode> selections = new ArrayList<SelectionNode>();
		ArrayList<SimpleExpression> selectionList = finder3.getSelectionList();
		for(int i=0;i<selectionList.size();++i){
			SelectionNode node3 = new SelectionNode();
			node3.setNodeName("Selection");
			node3.addConditon(selectionList.get(i));
			node3.setNodeID(this.nodeID);
			if(!selectionList.get(i).tableName.equalsIgnoreCase("null"))
				node3.setTableName(selectionList.get(i).tableName);
			else{
				if(tableList.size() == 1){
					node3.setTableName(tableList.get(0));
					node3.getCondList().get(0).tableName = tableList.get(0);
				}
				else
					node3.setTableName("null");
				node3.displayNode();
			}
			this.nodeID++;
			selections.add(node3);
		}
	
		//WhereClauseDecomposition p = new WhereClauseDecomposition(select);
		for(int i=0;i<joins.size();++i){
			JoinNode jnode = joins.get(i);
			TreeNode leftChild = findLeafNode(jnode.getLeftTableName(),leafs);
			TreeNode rightChild = findLeafNode(jnode.getRightTableName(),leafs);
			while(leftChild.getParent()!= null) leftChild = leftChild.getParent();
			while(rightChild.getParent()!=null) rightChild = rightChild.getParent();
			leftChild.setParent(jnode);
			rightChild.setParent(jnode);
		}
		for(int i=0;i<selections.size();++i){
			SelectionNode snode = selections.get(i);
			TreeNode child = findLeafNode(snode.getTableName(), leafs);
			while (child.getParent()!=null) child = child.getParent();
			child.setParent(snode);
		}
		TreeNode leaf1 = leafs.get(0);
		while(leaf1.getParent()!= null) leaf1 = leaf1.getParent();
		leaf1.setParent(curNode);
		
	
		/*
		for(int i = 0;i<leafs.size();++i)
		{
			localization(leafs.get(i));
		}
		
		setSiteIDOnNodes();
		*/
	}
	public void genDeleteTree(Delete delete){
		this.setTreeType(CONSTANT.TREE_DELETE);
		System.out.println(delete.toString());
		SelectionNode sel = new SelectionNode();
		WhereItemsFinder finder = new WhereItemsFinder(delete);
		ArrayList<SimpleExpression> selectionList = finder.getSelectionList();
		for(int i=0;i<selectionList.size();++i){
			selectionList.get(i).tableName = delete.getTable().getName();
			sel.addConditon(selectionList.get(i));
		}
		sel.setRoot();
		sel.setParent(null);
		sel.setTableName(delete.getTable().getName());
		sel.setSiteID(-1);
		sel.setNodeID(nodeID);
		nodeID++;
		root = sel;
		
		
		LeafNode leaf = new LeafNode();
		leaf.setNodeName(delete.getTable().getName());
		leaf.setTableName(delete.getTable().getName());
		leaf.setSegment(false);
		leaf.setNodeID(nodeID);
		nodeID++;
		leaf.setParent(sel);
		
		
		//localization(leaf);
		//setSiteIDOnNodes();
}
	
	public void genInsertTree(Insert insert){
		this.setTreeType(CONSTANT.TREE_INSERT);
		System.out.println(insert.getTable().getName());
		System.out.println(insert.getColumns().toString());
		System.out.println(insert.getItemsList().toString());
		ExpressionList itemList = (ExpressionList)insert.getItemsList();
		SelectionNode node = new SelectionNode();
		for(int i=0;i<insert.getColumns().size();++i){
			SimpleExpression e = new SimpleExpression();
			e.tableName = insert.getTable().getName();
			e.columnName = insert.getColumns().get(i).toString();
			e.op = "=";
			String value = itemList.getExpressions().get(i).toString();
			if(value.startsWith("\"")||value.startsWith("'")) value = value.substring(1,value.length()-1);
			e.value = value;
			if( itemList.getExpressions().get(i) instanceof LongValue){
				e.valueType = CONSTANT.VALUE_INT;
			}
			else if( itemList.getExpressions().get(i) instanceof DoubleValue){
				e.valueType = CONSTANT.VALUE_DOUBLE;
			}
			if( itemList.getExpressions().get(i) instanceof StringValue){
				e.valueType = CONSTANT.VALUE_STRING;
			}
			node.addConditon(e);
		}
		node.setParent(null);
		node.setRoot();
		node.setSiteID(-1);
		node.setNodeID(nodeID);
		nodeID++;
		root = node;
		
		LeafNode leaf = new LeafNode();
		leaf.setNodeName(insert.getTable().getName());
		leaf.setTableName(insert.getTable().getName());
		leaf.setSegment(false);
		leaf.setNodeID(nodeID);
		nodeID++;
		leaf.setParent(node);
		
		
		//localization(leaf);
		//setSiteIDOnNodes();
	}
	
	private LeafNode findLeafNode(String tableName,ArrayList<LeafNode> leafs){
		if (leafs.size() == 0) return null;
		for(int i=0;i<leafs.size();++i){
			if(leafs.get(i).getTableName().equalsIgnoreCase(tableName))
				return leafs.get(i);
		}
		return null;
	}
	private void localization(LeafNode leafNode){
		if(leafNode.hasSegmented()) return;
		UnionNode unionNode = new UnionNode();
		unionNode.setParent(leafNode.getParent());
		leafNode.getParent().removeChildNode(leafNode);
		leafNode.setParent(null);
		unionNode.setNodeID(leafNode.getNodeID());
		String tableName = leafNode.getTableName();
		GDD gdd = GDD.getInstance();
		List<String> subTableList = (List<String>)gdd.getTableFragList(tableName);
		for(int i=0;i<subTableList.size();++i){
			int siteID = gdd.getSiteNumberofFragmentation(subTableList.get(i));
			LeafNode node = new LeafNode();
			node.setTableName(subTableList.get(i));
			node.setNodeName(subTableList.get(i));
			node.setSegment(true);
			node.setNodeID(this.nodeID);
			node.setSiteID(siteID);
			this.nodeID++;
			node.setParent(unionNode);
		}
			
	}
	private void setSiteIDOnNodes(){
		if( this.root == null) return;
		setSiteIDOnNodesByChild(root);
	}
	private int setSiteIDOnNodesByChild(TreeNode node){
		if(node.isLeaf()) return node.getSiteID();
		List<TreeNode> childList = node.getChildList();
		for(int i=0;i<childList.size();++i)
		{
			setSiteIDOnNodesByChild(childList.get(i));
		}
		node.setSiteID(setSiteIDOnNodesByChild(node.getChild(0)));
		return node.getSiteID();
	}
	public void setLeafNodeList(List<LeafNode> leafNodeList) {
		this.leafNodeList = leafNodeList;
	}
	public List<LeafNode> getLeafNodeList() {
		if (this.root == null) return null;
		leafNodeList = new ArrayList<LeafNode>();
		getLeafNodeList(root);
		return leafNodeList;
	}
	private void getLeafNodeList(TreeNode node){
		if(node.isLeaf()){ 
			leafNodeList.add((LeafNode)node);
			return;
		}
		for(int i=0;i<node.getChildCount();++i){
			getLeafNodeList(node.getChild(i));
		}
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public String getSql() {
		return sql;
	}
	public void setClassID(int classID) {
		this.classID = classID;
	}
	public int getClassID() {
		return classID;
	}
}
