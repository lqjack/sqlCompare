package queryTree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import gdd.GDD;
import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;

public class TreeOptimizer {
	private QueryTree queryTree;
	private boolean treeOptimize = false;
	private boolean isNodeReduced = false;
	private boolean isNodeMerged = false;
	private boolean isJoinDownUnionUp = false;
	private boolean isAllTableJoin = false;
	private String minimalTableName = null;
	//tmp usage
	private ArrayList<JoinNode> joinList = new ArrayList<JoinNode>();
	private ArrayList<ProjectionNode> projectionList = new ArrayList<ProjectionNode>();
	private ArrayList<UnionNode> unionList = new ArrayList<UnionNode>();
	private ArrayList<SelectionNode> selectionList = new ArrayList<SelectionNode>();
	private ArrayList<LeafNode> leafList = new ArrayList<LeafNode>();
	private GDD gdd = GDD.getInstance();
	public TreeOptimizer(QueryTree queryTree)
	{
		this.queryTree = queryTree;
	}
	public void setQueryTree(QueryTree queryTree) {
		this.queryTree = queryTree;
	}

	public QueryTree getQueryTree(){
		return queryTree;
	}
	public void queryTreeOptimize()
	{
		if(queryTree.getLeafNodeList().size()==4)
			isAllTableJoin = true;
		if(isAllTableJoin){
			minimalTableName = getMinimalTableName();
		}
		localization();
		adapt_join_node();
		if(treeOptimize){
			rewriting();
			reduction();	
			removeUselessNode();
			mergeNode();
		}
		else{
			adapt_join_node();
		}
		adapt_join_node();
		siteDeployment();
		node_classification();
	}
	private void localization(){
		if(queryTree.getRoot()== null) return;
		ArrayList<LeafNode> leafNodeList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		for(int i=0;i<leafNodeList.size();++i)
		{
			localization(leafNodeList.get(i));
		}
	}
	private void localization(LeafNode leafNode)
	{
		String tableName = leafNode.getTableName();
		if(!gdd.isTableExist(tableName))
		{
			System.out.println("table "+tableName+" not exist!");
			return;
		}
		if (leafNode.hasSegmented()) 
			return;
		int fragType = gdd.getTableInfo(tableName).getFragType();
		switch (fragType)
		{	
			case CONSTANT.FRAG_HORIZONTAL: 
				UnionNode unionNode = new UnionNode();
				unionNode.setParent(leafNode.getParent());
				leafNode.getParent().removeChildNode(leafNode);
				leafNode.setParent(null);
				unionNode.setNodeID(leafNode.getNodeID());

				List<String> hSubTableList = (List<String>)gdd.getTableFragList(tableName);
				
				for(int i=0;i<hSubTableList.size();++i){
					int siteID = gdd.getSiteNumberofFragmentation(hSubTableList.get(i));
					LeafNode node = new LeafNode();
					node.setTableName(hSubTableList.get(i));
					node.setNodeName(hSubTableList.get(i));
					node.setSegment(true);
					node.setNodeID(queryTree.getNodeID());
					node.setSiteID(siteID);
					queryTree.setNodeID(queryTree.getNodeID()+1);
					node.setParent(unionNode);
				}
				break;
			case CONSTANT.FRAG_VERTICAL:
				String keyColName = "";

				for(int i=0;i<gdd.getTableInfo(tableName).getColNum();++i){
					if(gdd.getTableInfo(tableName).getColumnInfo().get(i).getColumnKeyable() == 1){
						keyColName = gdd.getTableInfo(tableName).getColumnInfo().get(i).getColumnName();
						break;
					}
				}
				List<String> vSubTableList = (List<String>)gdd.getTableFragList(tableName);
				ArrayList<LeafNode> newLeafList = new ArrayList<LeafNode>();
				for(int i=0;i<vSubTableList.size();++i){
					int siteID = gdd.getSiteNumberofFragmentation(vSubTableList.get(i));
					LeafNode node = new LeafNode();
					node.setTableName(vSubTableList.get(i));
					node.setNodeName(vSubTableList.get(i));
					node.setSegment(true);
					node.setNodeID(queryTree.getNodeID());
					node.setSiteID(siteID);
					queryTree.setNodeID(queryTree.getNodeID()+1);
					//node.setParent(joinNode);
					newLeafList.add(node);
				}
				if(newLeafList.size() == 0)
				{
					System.out.println("cannot localization table "+tableName);
					return;
				}
				if(newLeafList.size()==1)
				{
					LeafNode nd = newLeafList.get(0);
					nd.setParent(leafNode.getParent());
					leafNode.getParent().removeChildNode(leafNode);
					leafNode.setParent(null);
					nd.setNodeID(leafNode.getNodeID());
					return;
				}
				
				JoinNode joinNode= new JoinNode();
				joinNode.setParent(leafNode.getParent());
				leafNode.getParent().removeChildNode(leafNode);
				leafNode.setParent(null);
				joinNode.setNodeID(leafNode.getNodeID());
				joinNode.setSiteID(-1);
				LeafNode nd1 = newLeafList.get(0);
				LeafNode nd2 = newLeafList.get(1);
				nd1.setParent(joinNode);
				nd2.setParent(joinNode);
				joinNode.addAttribute(keyColName, keyColName);
				joinNode.setLeftTableName(nd1.getTableName());
				joinNode.setRightTableName(nd2.getTableName());
				LeafNode curNode = nd2;
				for(int i=2;i<newLeafList.size();++i)
				{
					JoinNode jNode= new JoinNode();
					joinNode.setParent(curNode.getParent());
					curNode.getParent().removeChildNode(curNode);
					curNode.setParent(jNode);
					jNode.setNodeID(curNode.getNodeID());
					LeafNode nd3 = newLeafList.get(i);
					nd3.setParent(jNode);
					jNode.addAttribute(keyColName, keyColName);
					jNode.setLeftTableName(curNode.getTableName());
					jNode.setRightTableName(nd3.getTableName());
					curNode = nd3;
				}
				break;
			case CONSTANT.FRAG_HYBIRD:
				//do nothing	
		}
	}
	private void reduction()
	{
		if(queryTree.getRoot()== null) return;
		ArrayList<LeafNode> leafNodeList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		if(leafNodeList.size() == 0) return;
		if(queryTree.getTreeType() == CONSTANT.TREE_DELETE){
			int fragType = gdd.getTableInfoFromFragName((leafNodeList.get(0).getTableName())).getFragType();
			if(fragType ==  CONSTANT.FRAG_VERTICAL)
				return;
		}	
		for(int i=0;i<leafNodeList.size();++i)
		{	
			int fragType = gdd.getTableInfoFromFragName((leafNodeList.get(i).getTableName())).getFragType();
			if(fragType == CONSTANT.FRAG_HORIZONTAL)
				horizontalReduction(leafNodeList.get(i));
			else if(fragType ==  CONSTANT.FRAG_VERTICAL){
				System.out.println("PPPpp  "+leafNodeList.get(i).getTableName());
			    verticalReduction(leafNodeList.get(i));
			}
			else if(fragType == CONSTANT.FRAG_HYBIRD)
			{
				//to  do
			}
		}
	}
	private void horizontalReduction(TreeNode node)
	{
		boolean isRedundant  = false;
		TreeNode parent = node.getParent();
		while(parent!=null){
			if(parent instanceof SelectionNode)
			{
				if(reductionWithSelection(node,parent)){
					isRedundant = true;
					break;
				}
			}
			else
			{
				//to do
			}
			parent = parent.getParent();
		}
		isRedundant = false;
		if(isRedundant)
		{
			if(node.getParent()!=null){
				node.getParent().removeChildNode(node);
				node.setParent(null);
			}
		}
	}
	private boolean reductionWithSelection(TreeNode child,TreeNode parent)
	{
		LeafNode lNode = (LeafNode)child;
		SelectionNode sNode = (SelectionNode)parent;
		if (gdd.getFragmentation(lNode.getTableName()).getFragType() == CONSTANT.FRAG_VERTICAL)
			return false;
		List<SimpleExpression> fragCondList = gdd.getFragmentation(lNode.getTableName()).getFragConditionExpression().HorizontalFragmentationCondition;
		ArrayList<SimpleExpression> condList = (ArrayList<SimpleExpression>) sNode.getCondList();
		for(int i=0;i<fragCondList.size();++i)
		{
			for(int j=0;j<condList.size();++j)
			{
				if(isConditionConfilct(fragCondList.get(i),condList.get(j))){
					return true;
				}
			}
		}
		return false;
	}
	private boolean reductionWithJoin(LeafNode lNode,JoinNode jNode)
	{
		return false;
	}
	private void verticalReduction(TreeNode node){
		boolean isRedundant  =true;
		TreeNode parent = node.getParent();
		/*
		while(parent!=null){
			if(reductionInVerticalFragment(node,parent)){
				isRedundant = false;
				break;
			}
			parent = parent.getParent();
		}
		if(isRedundant)
		{
			if(node.getParent()!=null){
				node.getParent().removeChildNode(node);
				node.setParent(null);
			}
		}
		*/
		while(parent!=null){
			if(reductionInVerticalFragment(node,parent)){
				parent = parent.getParent();
			}
			else{
				if(parent instanceof SelectionNode){
					TreeNode newParent = parent.getParent();
					TreeNode firstChild = parent.getChild(0);
					firstChild.setParent(newParent);
					if(newParent !=null){
						newParent.removeChildNode(parent);
						parent.setParent(null);
					}
					parent = newParent;
				}
				else if(parent instanceof ProjectionNode){
					/*
					TreeNode newParent = node.getParent();
					boolean sign = true;
					while(newParent!= parent){
						if(newParent.getChildCount() != 1){
							sign = false;
							break;
						}
						newParent = newParent.getParent();
					}
					if(sign){
						System.out.println("OK");
						newParent = parent.getParent();
						TreeNode firstChild = parent.getChild(0);
						firstChild.setParent(newParent);
						if(newParent !=null){
							newParent.removeChildNode(parent);
							parent.setParent(null);
						}
						parent = newParent;
					}
					else{
						parent = parent.getParent();
					}*/
					parent = parent.getParent();
				}
				else{
					parent = parent.getParent();
				}
			}
		}
	}
	private boolean reductionInVerticalFragment(TreeNode child,TreeNode parent){
		LeafNode lNode = (LeafNode)child;
		List<String> subColumnList = gdd.getFragmentation(lNode.getTableName()).getFragConditionExpression().verticalFragmentationCondition;
		String tName = gdd.getTableInfoFromFragName(lNode.getTableName()).getTableName();
		if(parent instanceof JoinNode){
			JoinNode jNode = (JoinNode)parent;
			String lTableName = jNode.getLeftTableName();
			String rTableName = jNode.getRightTableName();
			for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
				String key = i.next(); 
				String value = jNode.getAttributeList().get(key); 
				for(int j=0;j<subColumnList.size();++j){
					if(subColumnList.get(j).equals(key)&&tName.equals(lTableName) && !gdd.isKeyOfTable(tName,subColumnList.get(j)))
						return true;
					if(subColumnList.get(j).equals(value)&&tName.equals(rTableName)&& !gdd.isKeyOfTable(tName,subColumnList.get(j)))
						return true;
				}
			}
		}
		else if(parent instanceof ProjectionNode){
			ProjectionNode pNode = (ProjectionNode)parent;
			for(int i=0;i<pNode.getAttributeNum();++i){
				String attr = pNode.getAttributeList().get(i);
				String table = pNode.getTableNameList().get(i);
				
				for(int j=0;j<subColumnList.size();++j){
					if(tName.equals(table)){
						if(subColumnList.get(j).equals(attr)&& !gdd.isKeyOfTable(tName,subColumnList.get(j)))
							return true;
					}
					if(attr.equals("*")&&!gdd.isKeyOfTable(tName,subColumnList.get(j))){
						return true;
					}
				}
				
			}
		}
		else if(parent instanceof SelectionNode){
			SelectionNode sNode = (SelectionNode)parent;
			sNode.getCondList().get(0).displayResult();
			for(int i=0;i<sNode.getCondList().size();++i){
				SimpleExpression exp = sNode.getCondList().get(i);
				String suTableName = gdd.getTableNameofFragmentation(exp.tableName);
				if(exp.tableName.equals(tName)||(suTableName!=null && suTableName.equals(tName))){
					for(int j=0;j<subColumnList.size();++j){
						if(subColumnList.get(j).equalsIgnoreCase(exp.columnName)&& !gdd.isKeyOfTable(tName,subColumnList.get(j)))
							return true;
						if(subColumnList.get(j).equalsIgnoreCase(exp.columnName))
                            return true;
					}
				}
			}
		}
		return false;
	}
	
	private void rewriting()
	{
		if(queryTree.getRoot() == null) return;
		if(queryTree.getTreeType() == CONSTANT.TREE_SELECT)
			rewriting_projection((ProjectionNode)queryTree.getRoot());
		rewriting(queryTree.getRoot());
		rewriting_join_down_union_up();
	}
	private void rewriting(TreeNode node){
		if (node.isLeaf()) return;
		for(int i=0; i<node.getChildCount();++i){
			TreeNode child = node.getChild(i);
			if(child instanceof SelectionNode){
				rewriting_selection_down((SelectionNode)child);
			}
			else if(child instanceof JoinNode){
				rewriting_join((JoinNode)child);
			}
			else if(child instanceof ProjectionNode){
				rewriting_projection((ProjectionNode)child);
			}
			rewriting(child);
		}
		//remove ProjectionNode generated by JOIN
		for(int i=0; i<node.getChildCount();++i){
			TreeNode child = node.getChild(i);
			if(child instanceof ProjectionNode && child.getNodeName().equalsIgnoreCase("Projection_JOIN")){
				TreeNode newChild = child.getChild(0);
				newChild.setParent(child.getParent());
				child.getParent().removeChildNode(child);
				child.setParent(null);
				i--;
			}
		}
	}
	private void rewriting_projection(ProjectionNode pNode){
		if(pNode.getTableNameList().size() == 0) return;
		if(pNode.getAttributeList().size() == 0) return;
		if(gdd.getTableNameofFragmentation(pNode.getTableNameList().get(0)) != null) return;
		String pTableName = pNode.getTableNameList().get(0);
		String pAttrName = pNode.getAttributeList().get(0);
		ArrayList<LeafNode> leafNodeList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		for(int i = 0;i<leafNodeList.size();++i)
		{
			LeafNode leaf = leafNodeList.get(i);
			String suTableName = gdd.getTableNameofFragmentation(leaf.getTableName());
			if(!suTableName.equalsIgnoreCase(pTableName))
				continue;
			Vector<String> columnList = gdd.getTableInfoFromFragName(leaf.getTableName()).getColumnNames();
			if(gdd.getTableInfoFromFragName(leaf.getTableName()).getFragType() == CONSTANT.FRAG_VERTICAL){
				columnList = gdd.getFragmentation(leaf.getTableName()).getFragConditionExpression().verticalFragmentationCondition;
			}
			boolean sign = false;
			for(int j=0;j<columnList.size();++j){
				if(columnList.get(j).equalsIgnoreCase(pAttrName)){
					sign = true;
					break;
				}
				else if(pAttrName.equalsIgnoreCase("*"))
				{
					sign = true;
					break;
				}
			}
			if(sign){
				ProjectionNode newPNode = new ProjectionNode();
				newPNode.setNodeName("Projection");
				newPNode.addAttribute(pAttrName);
				newPNode.addTableName(leaf.getTableName());
				newPNode.setNodeID(queryTree.getNodeID());
				queryTree.setNodeID(queryTree.getNodeID()+1);
				TreeNode curNode = leaf;
				while(curNode.getParent() instanceof SelectionNode)
					curNode = curNode.getParent();
				newPNode.setParent(curNode.getParent());
				curNode.getParent().removeChildNode(curNode);
				curNode.setParent(newPNode);
			}
		}
	}
	private void rewriting_join(JoinNode jNode)
	{
		if(jNode.getChildCount() < 2) return;
		String lTableName = jNode.getLeftTableName();
		String rTableName = jNode.getRightTableName();
		TreeNode lChild = jNode.getChild(0);
		TreeNode rChild = jNode.getChild(1);
		for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
			String key = i.next(); 
			String value = jNode.getAttributeList().get(key); 
			//System.out.println("key = "+lTableName+"."+key+" ;value="+rTableName+"."+value);
			ProjectionNode lPNode = new ProjectionNode();
			lPNode.setNodeName("Projection_JOIN");
			lPNode.addAttribute(key);
			lPNode.addTableName(lTableName);
			lPNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			lPNode.setParent(lChild.getParent());
			lChild.getParent().removeChildNode(lChild);
			lChild.setParent(lPNode);
			ProjectionNode rPNode = new ProjectionNode();
			rPNode.setNodeName("Projection_JOIN");
			rPNode.addAttribute(value);
			rPNode.addTableName(rTableName);
			rPNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			rPNode.setParent(rChild.getParent());
			rChild.getParent().removeChildNode(rChild);
			rChild.setParent(rPNode);
		}
	}

	private void rewriting_join_down_union_up(){
		if(queryTree.getRoot() == null) return;
		isJoinDownUnionUp = true;
		while(isJoinDownUnionUp){
			isJoinDownUnionUp = false;
			rewriting_join_down_union_up(queryTree.getRoot());
		}
	}
	private void rewriting_join_down_union_up(TreeNode node){
		if(isJoinDownUnionUp) return;
		if(node.isLeaf())return;
		if(!(node instanceof JoinNode)){
			for(int i=0;i<node.getChildCount();++i)
				rewriting_join_down_union_up(node.getChild(i));
			return;
		}
		JoinNode jNode = (JoinNode)node;
		if(jNode.getChildCount() != 2) return;
		TreeNode leftChild = jNode.getChild(0);
		TreeNode rightChild = jNode.getChild(1);
		if(leftChild instanceof UnionNode && rightChild instanceof UnionNode){
			//if(isNoJoinNodeBellow(leftChild)&&isNoJoinNodeBellow(rightChild))
				//return;
			if(isAllTableJoin&&isNoJoinNodeBellow(leftChild)){
				TreeNode tmpNode = leftChild;
				while(!tmpNode.isLeaf())tmpNode = tmpNode.getChild(0);
				LeafNode lNode =(LeafNode)tmpNode;
				if(lNode.getTableName().startsWith(minimalTableName)){
					UnionNode uNode = (UnionNode)rightChild;
					uNode.setParent(jNode.getParent());
					jNode.getParent().removeChildNode(jNode);
					jNode.setParent(null);
					int size = uNode.getChildCount();
					while(size>0){
						TreeNode cpyNode = copyTree(leftChild);
						TreeNode child = uNode.getChild(0);
						JoinNode newJNode = new JoinNode();
						newJNode.setNodeID(queryTree.getNodeID());
						queryTree.setNodeID(queryTree.getNodeID()+1);
						newJNode.setParent(uNode);
						newJNode.setNodeName("JOIN");
						newJNode.setSiteID(-1);
						
						newJNode.setLeftTableName(jNode.getLeftTableName());
						newJNode.setRightTableName(jNode.getRightTableName());
						if(jNode.getAttributeList().size()>0){
							for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
								String key = i.next(); 
								String value = jNode.getAttributeList().get(key); 
								newJNode.addAttribute(key,value);
							}
						}
						cpyNode.setParent(newJNode);
						child.getParent().removeChildNode(child);
						child.setParent(newJNode);
						size --;
					}
					isJoinDownUnionUp = true;
					return;
				}
			}
			if(isAllTableJoin&&isNoJoinNodeBellow(rightChild)){
				TreeNode tmpNode = rightChild;
				while(!tmpNode.isLeaf())tmpNode = tmpNode.getChild(0);
				LeafNode lNode =(LeafNode)tmpNode;
				if(lNode.getTableName().startsWith(minimalTableName)){
					UnionNode uNode = (UnionNode)leftChild;
					uNode.setParent(jNode.getParent());
					jNode.getParent().removeChildNode(jNode);
					jNode.setParent(null);
					int size = uNode.getChildCount();
					while(size>0){
						TreeNode cpyNode = copyTree(rightChild);
						TreeNode child = uNode.getChild(0);
						JoinNode newJNode = new JoinNode();
						newJNode.setNodeID(queryTree.getNodeID());
						queryTree.setNodeID(queryTree.getNodeID()+1);
						newJNode.setParent(uNode);
						newJNode.setNodeName("JOIN");
						newJNode.setSiteID(-1);
						
						newJNode.setLeftTableName(jNode.getLeftTableName());
						newJNode.setRightTableName(jNode.getRightTableName());
						if(jNode.getAttributeList().size()>0){
							for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
								String key = i.next(); 
								String value = jNode.getAttributeList().get(key); 
								newJNode.addAttribute(key,value);
							}
						}
						child.getParent().removeChildNode(child);
						child.setParent(newJNode);
						cpyNode.setParent(newJNode);
						size --;
					}
					isJoinDownUnionUp = true;
					return;
				}
			}
			UnionNode lUNode = (UnionNode)leftChild;
			UnionNode rUNode = (UnionNode)rightChild;
			lUNode.setParent(jNode.getParent());
			jNode.getParent().removeChildNode(jNode);
			jNode.setParent(null);
			int leftSize = lUNode.getChildCount();
			int rightSize = rUNode.getChildCount();
			while(leftSize>0){
				for(int i=0;i<rightSize;++i){
					TreeNode cpyLNode = copyTree(lUNode.getChild(0));
					TreeNode cpyRNode = copyTree(rUNode.getChild(i));
					JoinNode newJNode = new JoinNode();
					newJNode.setNodeID(queryTree.getNodeID());
					queryTree.setNodeID(queryTree.getNodeID()+1);
					newJNode.setParent(lUNode);
					newJNode.setNodeName("JOIN");
					newJNode.setSiteID(-1);
					newJNode.setLeftTableName(jNode.getLeftTableName());
					newJNode.setRightTableName(jNode.getRightTableName());
					if(jNode.getAttributeList().size()>0){
						for (Iterator<String> it = jNode.getAttributeList().keySet().iterator(); it.hasNext();) { 
							String key = it.next(); 
							String value = jNode.getAttributeList().get(key);
							newJNode.addAttribute(key,value);
						}
					}
					cpyLNode.setParent(newJNode);
					cpyRNode.setParent(newJNode);
				}
				lUNode.getChild(0).setParent(null);
				lUNode.removeChildNode(lUNode.getChild(0));
				leftSize --;
			}
			isJoinDownUnionUp = true;
			
			return;
		}
		else if(leftChild instanceof UnionNode){
			if(isAllTableJoin&&isNoJoinNodeBellow(leftChild)){
				TreeNode tmpNode = leftChild;
				while(!tmpNode.isLeaf())tmpNode = tmpNode.getChild(0);
				LeafNode lNode =(LeafNode)tmpNode;
				if(lNode.getTableName().startsWith(minimalTableName)){
					for(int i=0;i<rightChild.getChildCount();++i)
						rewriting_join_down_union_up(rightChild.getChild(i));
					return;
				}
			}
			UnionNode uNode = (UnionNode)leftChild;
			uNode.setParent(jNode.getParent());
			jNode.getParent().removeChildNode(jNode);
			jNode.setParent(null);
			int size = uNode.getChildCount();
			while(size>0){
				TreeNode cpyNode = copyTree(rightChild);
				TreeNode child = uNode.getChild(0);
				JoinNode newJNode = new JoinNode();
				newJNode.setNodeID(queryTree.getNodeID());
				queryTree.setNodeID(queryTree.getNodeID()+1);
				newJNode.setParent(uNode);
				newJNode.setNodeName("JOIN");
				newJNode.setSiteID(-1);
				
				newJNode.setLeftTableName(jNode.getLeftTableName());
				newJNode.setRightTableName(jNode.getRightTableName());
				if(jNode.getAttributeList().size()>0){
					for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
						String key = i.next(); 
						String value = jNode.getAttributeList().get(key); 
						newJNode.addAttribute(key,value);
					}
				}
				child.getParent().removeChildNode(child);
				child.setParent(newJNode);
				cpyNode.setParent(newJNode);
				size --;
			}
			for(int i=0;i<rightChild.getChildCount();++i)
				rewriting_join_down_union_up(rightChild.getChild(i));
			isJoinDownUnionUp = true;
			return;
		}
		else if(rightChild instanceof UnionNode){
			if(isAllTableJoin&&isNoJoinNodeBellow(rightChild)){
				TreeNode tmpNode = rightChild;
				while(!tmpNode.isLeaf())tmpNode = tmpNode.getChild(0);
				LeafNode lNode =(LeafNode)tmpNode;
				if(lNode.getTableName().startsWith(minimalTableName)){
					for(int i=0;i<leftChild.getChildCount();++i)
						rewriting_join_down_union_up(leftChild.getChild(i));
					return;
				}
			}
			UnionNode uNode = (UnionNode)rightChild;
			uNode.setParent(jNode.getParent());
			jNode.getParent().removeChildNode(jNode);
			jNode.setParent(null);
			int size = uNode.getChildCount();
			while(size>0){
				TreeNode cpyNode = copyTree(leftChild);
				TreeNode child = uNode.getChild(0);
				JoinNode newJNode = new JoinNode();
				newJNode.setNodeID(queryTree.getNodeID());
				queryTree.setNodeID(queryTree.getNodeID()+1);
				newJNode.setParent(uNode);
				newJNode.setNodeName("JOIN");
				newJNode.setSiteID(-1);
				
				newJNode.setLeftTableName(jNode.getLeftTableName());
				newJNode.setRightTableName(jNode.getRightTableName());
				if(jNode.getAttributeList().size()>0){
					for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
						String key = i.next(); 
						String value = jNode.getAttributeList().get(key); 
						newJNode.addAttribute(key,value);
					}
				}
				cpyNode.setParent(newJNode);
				child.getParent().removeChildNode(child);
				child.setParent(newJNode);
				size --;
			}
			for(int i=0;i<leftChild.getChildCount();++i)
				rewriting_join_down_union_up(leftChild.getChild(i));
			isJoinDownUnionUp = true;
			return;
		}
		else{
			for(int i=0;i<jNode.getChildCount();++i)
				rewriting_join_down_union_up(jNode.getChild(i));
		}
	}
	private void rewriting_selection_down(SelectionNode sNode){
		if(sNode.getChildCount() == 0) return;
		if(sNode.getChild(0).isLeaf()) return;
		if(gdd.getTableNameofFragmentation(sNode.getTableName())!=null) return;
		TreeNode parent = sNode.getParent();
		TreeNode child = sNode.getChild(0);
		ArrayList<LeafNode> leafNodeList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		for(int i = 0;i<leafNodeList.size();++i)
		{
			LeafNode leaf = leafNodeList.get(i);
			String suTableName = gdd.getTableNameofFragmentation(leaf.getTableName());
			if(suTableName == null) continue;
			boolean sign = true;
			if(gdd.getTableInfo(suTableName).getFragType() == CONSTANT.FRAG_VERTICAL){
				Vector<String> columnList = gdd.getFragmentation(leaf.getTableName()).getFragConditionExpression().verticalFragmentationCondition;
				sign = false;
				for(int j=0;j<columnList.size();++j){
					if(columnList.get(j).equalsIgnoreCase(sNode.getCondList().get(0).columnName)){
						sign = true;
						break;
					}
				}
			}
			boolean sign2 = true;
			if(gdd.getTableInfo(suTableName).getFragType() == CONSTANT.FRAG_HORIZONTAL){
				Vector<SimpleExpression> expList = gdd.getFragmentation(leaf.getTableName()).getFragConditionExpression().HorizontalFragmentationCondition;
				SimpleExpression exp2 = sNode.getCondList().get(0);
				for(int j=0;j<expList.size();++j){
					SimpleExpression exp1 = expList.get(j);
					if(isConditionRedundant(exp1,exp2)){
						sign2 = false;
						break;
					}
				}
			}
			if(suTableName.equalsIgnoreCase(sNode.getTableName())&& sign && sign2){
				SelectionNode sNode2 = new SelectionNode();
				for(int j=0;j<sNode.getCondList().size();++j){
					SimpleExpression exp = new SimpleExpression();
					exp.columnName = sNode.getCondList().get(j).columnName;
					exp.op = sNode.getCondList().get(j).op;
					exp.value = sNode.getCondList().get(j).value;
					exp.valueType = sNode.getCondList().get(j).valueType;
					exp.tableName = leaf.getTableName();
					sNode2.addConditon(exp);
				}
				sNode2.setNodeName(leaf.getTableName());
				sNode2.setNodeID(queryTree.getNodeID());
				queryTree.setNodeID(queryTree.getNodeID()+1);
				sNode2.setTableName(leaf.getTableName());
				sNode2.setParent(leaf.getParent());
				leaf.getParent().removeChildNode(leaf);
				leaf.setParent(sNode2);
			}
		}
		if(parent == null)
		{
			child.setParent(null);
			child.setRoot();
			queryTree.setRoot(child);
		}
		else
		{
			child.setParent(sNode.getParent());
			sNode.getParent().removeChildNode(sNode);
			sNode.setParent(null);
		}
		
	}
	private void removeUselessNode()
	{
		if(queryTree.getRoot()== null) return;
		ArrayList<LeafNode> leafNodeList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		if(leafNodeList.size() == 0) return;
		if(queryTree.getTreeType() == CONSTANT.TREE_DELETE){
			int fragType = gdd.getTableInfoFromFragName((leafNodeList.get(0).getTableName())).getFragType();
			if(fragType ==  CONSTANT.FRAG_VERTICAL)
				return;
		}	
		isNodeReduced = true;
		while(isNodeReduced){
			isNodeReduced = false;
			removeUselessNode(queryTree.getRoot());
		}
	}
	private void removeUselessNode(TreeNode node){
		if(node.isLeaf()) return;
		//if(node instanceof SelectionNode){
		    //String table = ((SelectionNode)node).getTableName();
		    //if(gdd.getTableInfoFromFragName(table).getFragType() == CONSTANT.FRAG_VERTICAL)
		   // return;
		//}
		for(int i=0;i<node.getChildCount();++i)
		{
			removeUselessNode(node.getChild(i));
		}
		
		if(node.getChildCount() == 0)
		{
			if(node.getParent()!= null)
				node.getParent().removeChildNode(node);
			node.setParent(null);
			isNodeReduced = true;
			return;
		}
		
		if(node instanceof UnionNode)
		{
	
			UnionNode uNode = (UnionNode)node;
			if(uNode.getChildCount() == 1)
			{
				TreeNode child = uNode.getChild(0);
				child.setParent(uNode.getParent());
				if(uNode.isRoot())
				{
					queryTree.setRoot(child);
					child.setRoot();
				}
				else
				{
					uNode.getParent().removeChildNode(uNode);
					uNode.setParent(null);
				}
				isNodeReduced = true;
			}
			
		}
		else if(node instanceof JoinNode)
		{
			JoinNode jNode = (JoinNode) node;
			if(jNode.getChildCount() == 1)
			{
				/*
				TreeNode child = jNode.getChild(0);
				child.setParent(jNode.getParent());
				if(jNode.isRoot())
				{
					queryTree.setRoot(child);
					child.setRoot();
				}
				else
				{
					jNode.getParent().removeChildNode(jNode);	
					jNode.setParent(null);
				}
				*/
				jNode.getParent().removeChildNode(jNode);
				jNode.setParent(null);
				isNodeReduced = true;
			}
			else if(jNode.getChildCount() == 2){
				if(is_join_node_redundant(jNode)){
					jNode.getParent().removeChildNode(jNode);
					jNode.setParent(null);
					isNodeReduced = true;
				}
				else{
					int result = is_join_vertical_redudant(jNode);
					if(result>0){
						if(result==1){
							TreeNode child = jNode.getChild(1);
							child.setParent(jNode.getParent());
							if(jNode.getParent()!=null)
								jNode.getParent().removeChildNode(jNode);
							if(child.getParent()!=null && child.getParent() instanceof JoinNode){
								JoinNode tmp = (JoinNode)child.getParent();
								String tmpName = tmp.getLeftTableName();
								tmp.setLeftTableName(tmp.getRightTableName());
								tmp.setRightTableName(tmpName);
								Iterator<String> it = tmp.getAttributeList().keySet().iterator(); 
								String key = it.next();
								String value = tmp.getAttributeList().get(key);
								tmp.getAttributeList().remove(key);
								tmp.addAttribute(value, key);
							}
							jNode.setParent(null);
						}
						else{
							TreeNode child = jNode.getChild(0);
							child.setParent(jNode.getParent());
							if(jNode.getParent()!=null)
								jNode.getParent().removeChildNode(jNode);
							if(child.getParent()!=null && child.getParent() instanceof JoinNode){
								JoinNode tmp = (JoinNode)child.getParent();
								String tmpName = tmp.getLeftTableName();
								tmp.setLeftTableName(tmp.getRightTableName());
								tmp.setRightTableName(tmpName);
								Iterator<String> it = tmp.getAttributeList().keySet().iterator(); 
								String key = it.next();
								String value = tmp.getAttributeList().get(key);
								tmp.getAttributeList().remove(key);
								tmp.addAttribute(value, key);
							}
							jNode.setParent(null);
						}
					}
				}
			}
		}
		else if(node instanceof ProjectionNode)
		{
			ProjectionNode pNode = (ProjectionNode)node;
			if (pNode.getChildCount() == 0) return;
			TreeNode curNode = pNode.getParent();
			TreeNode child = pNode.getChild(0);
			boolean sign = false;
			while(curNode != null)
			{
				if(curNode instanceof ProjectionNode)
				{
					ProjectionNode curPNode = (ProjectionNode)curNode;
					if(curPNode.getAttributeList().get(0).equalsIgnoreCase(pNode.getAttributeList().get(0))
							&& curPNode.getTableNameList().get(0).equalsIgnoreCase(pNode.getTableNameList().get(0)))
					{
						sign = true;
						break;
					}
				}
				curNode = curNode.getParent();
			}
			if(sign)
			{
				child.setParent(pNode.getParent());
				pNode.getParent().removeChildNode(pNode);
				pNode.setParent(null);
			}
			if(pNode.getAttributeNum() == 1)
			{
				/*if(pNode.getAttributeList().get(0).equalsIgnoreCase("*"))
				{
					TreeNode child = pNode.getChild(0);
					child.setParent(pNode.getParent());
					if(pNode.isRoot()){
						queryTree.setRoot(child);
					}
					else{
						pNode.getParent().removeChildNode(pNode);
						pNode.setParent(null);
					}
					isNodeReduced = true;
				}*/
			}
			
		}
		else 
		{
			//to do
		}
		
	}
	private int is_join_vertical_redudant(JoinNode jNode){
		if(hasJoinNode(jNode.getChild(0))||hasJoinNode(jNode.getChild(1)))
			return 0;
		String leftTableName = jNode.getLeftTableName();
		String rightTableName = jNode.getRightTableName();
		if(gdd.getTableNameofFragmentation(leftTableName)== null)
			return 0;
		if(gdd.getTableNameofFragmentation(rightTableName)== null)
			return 0;
		if(!gdd.getTableNameofFragmentation(leftTableName).equalsIgnoreCase(gdd.getTableNameofFragmentation(rightTableName)))
			return 0;
		if(gdd.getTableInfoFromFragName(leftTableName).getFragType()!=CONSTANT.FRAG_VERTICAL)
			return 0;
		ArrayList<ProjectionNode> lPNodeList = new ArrayList<ProjectionNode>();
		ArrayList<SelectionNode> lSNodeList = new ArrayList<SelectionNode>();
		ArrayList<ProjectionNode> rPNodeList = new ArrayList<ProjectionNode>();
		ArrayList<SelectionNode> rSNodeList = new ArrayList<SelectionNode>();
		TreeNode curNode = jNode.getChild(0);
		while(!curNode.isLeaf()){
			if(curNode instanceof ProjectionNode)
				lPNodeList.add((ProjectionNode) curNode);
			else if(curNode instanceof SelectionNode)
				lSNodeList.add((SelectionNode) curNode);
			curNode = curNode.getChild(0);
		}
		
		if(lPNodeList.size()==1 && lSNodeList.size()==0){
			String attr = lPNodeList.get(0).getAttributeList().get(0);
			if(gdd.isKeyOfTable(gdd.getTableNameofFragmentation(leftTableName), attr))
				return 1;
		}
		else if(lPNodeList.size()==0 && lSNodeList.size()==1){
			String attr = lSNodeList.get(0).getCondList().get(0).columnName;
			if(gdd.isKeyOfTable(gdd.getTableNameofFragmentation(leftTableName), attr))
				return 1;
		}
		else if(lPNodeList.size()==0 && lSNodeList.size()==0)
			return 1;
		else if(lPNodeList.size()==1 && lSNodeList.size()==1){
			String attr1 = lPNodeList.get(0).getAttributeList().get(0);
			String attr2 = lSNodeList.get(0).getCondList().get(0).columnName;
			if(gdd.isKeyOfTable(gdd.getTableNameofFragmentation(leftTableName), attr1)&& attr1.equalsIgnoreCase(attr2))
				return 1;
		}
		curNode = jNode.getChild(1);
		while(!curNode.isLeaf()){
			if(curNode instanceof ProjectionNode)
				rPNodeList.add((ProjectionNode) curNode);
			else if(curNode instanceof SelectionNode)
				rSNodeList.add((SelectionNode) curNode);
			curNode = curNode.getChild(0);
		}
		System.out.println("LLLL    "+rPNodeList.size()+" "+rSNodeList.size());
		if(rPNodeList.size()==1 && rSNodeList.size()==0){
			String attr = rPNodeList.get(0).getAttributeList().get(0);
			if(gdd.isKeyOfTable(gdd.getTableNameofFragmentation(rightTableName), attr))
				return 2;
			    //return 0;
		}
		else if(rPNodeList.size()==0 && rSNodeList.size()==1){
			String attr = rSNodeList.get(0).getCondList().get(0).columnName;
			if(gdd.isKeyOfTable(gdd.getTableNameofFragmentation(rightTableName), attr))
				return 2;
		}
		else if(rPNodeList.size()==0 && rSNodeList.size()==0)
			return 2;
		else if(rPNodeList.size()==1 && rSNodeList.size()==1){
			String attr1 = rPNodeList.get(0).getAttributeList().get(0);
			String attr2 = rSNodeList.get(0).getCondList().get(0).columnName;
			if(gdd.isKeyOfTable(gdd.getTableNameofFragmentation(rightTableName), attr1)&& attr1.equalsIgnoreCase(attr2))
				return 2;
		}
		return 0;
	}
	private boolean is_join_node_redundant(JoinNode jNode){
		if(hasJoinNode(jNode.getChild(0))||hasJoinNode(jNode.getChild(1)))
			return false;
		String leftTableName = jNode.getLeftTableName();
		String rightTableName = jNode.getRightTableName();
		if(gdd.getTableNameofFragmentation(leftTableName)== null){
			if(gdd.getTableInfo(leftTableName).getFragType()== CONSTANT.FRAG_VERTICAL)
				return false;
			TreeNode node = jNode.getChild(0);
			while(!node.isLeaf())node = node.getChild(0);
			leftTableName = ((LeafNode)node).getTableName();
			
		}
		else{
			if(gdd.getTableInfoFromFragName(leftTableName).getFragType()== CONSTANT.FRAG_VERTICAL)
				return false;
		}
		
		if(gdd.getTableNameofFragmentation(rightTableName)== null){
			if(gdd.getTableInfo(rightTableName).getFragType()== CONSTANT.FRAG_VERTICAL)
				return false;
			TreeNode node = jNode.getChild(1);
			while(!node.isLeaf())node = node.getChild(0);
			rightTableName = ((LeafNode)node).getTableName();
		}
		else{
			if(gdd.getTableInfoFromFragName(rightTableName).getFragType()== CONSTANT.FRAG_VERTICAL)
				return false;
		}
		Vector<SimpleExpression> leftExpList = gdd.getFragmentation(leftTableName).getFragConditionExpression().HorizontalFragmentationCondition;
		Vector<SimpleExpression> rightExpList = gdd.getFragmentation(rightTableName).getFragConditionExpression().HorizontalFragmentationCondition;
		TreeNode curNode = jNode.getChild(0);
		while(!curNode.isLeaf()){
			if(curNode instanceof SelectionNode){
				SelectionNode tmp = (SelectionNode)curNode;
				for(int i=0;i<tmp.getCondList().size();++i){
					leftExpList.add(tmp.getCondList().get(i));
				}
			}
			curNode = curNode.getChild(0);
		}
		curNode = jNode.getChild(1);
		while(!curNode.isLeaf()){
			if(curNode instanceof SelectionNode){
				SelectionNode tmp = (SelectionNode)curNode;
				for(int i=0;i<tmp.getCondList().size();++i){
					rightExpList.add(tmp.getCondList().get(i));
				}
			}
			curNode = curNode.getChild(0);
		}
		Iterator<String> it = jNode.getAttributeList().keySet().iterator();
		String leftColumnName = it.next(); 
		String rightColumnName = jNode.getAttributeList().get(leftColumnName); 
		for(int i=0;i<leftExpList.size();++i){
			for(int j=0;j<rightExpList.size();++j){
				SimpleExpression exp1 = leftExpList.get(i);
				SimpleExpression exp2 = rightExpList.get(j);
				if(exp1.columnName.equalsIgnoreCase(leftColumnName)&& exp2.columnName.equalsIgnoreCase(rightColumnName)
						||exp1.columnName.equalsIgnoreCase(rightColumnName)&& exp2.columnName.equalsIgnoreCase(leftColumnName)){
					SimpleExpression exp3 = new SimpleExpression();
					exp3.tableName = exp1.tableName;
					exp3.columnName = exp1.columnName;
					exp3.op = exp2.op;
					exp3.value = exp2.value;
					exp3.valueType = exp2.valueType;
					if(isConditionConfilct(exp1,exp3)){
						return true;
					}
				}
			}	
		}
		return false;
	}
	private boolean hasJoinNode(TreeNode node){
		if (node.isLeaf()) return false;
		if (node instanceof JoinNode) return true;
		for(int i=0;i<node.getChildCount();++i){
			if(hasJoinNode(node.getChild(i)))
				return true;
		}
		return false;
	}
	private void mergeNode()
	{
		//������ͬ��table�����������selection Node�ϳ�һ��(farther and child)
		if(queryTree.getRoot() == null) return;
		mergeNode_from_bottom();
		//mergeNode_from_top();
		//adapt_join_node();
	}
	private void mergeNode_from_top(){
		ArrayList<TreeNode> candList = new ArrayList<TreeNode>();
		TreeNode curNode = queryTree.getRoot();
		if(((ProjectionNode)curNode).getAttributeList().size()>1)
			return;
		while(curNode != null && curNode instanceof ProjectionNode){
			boolean sign= true;
			for(int i=0;i<candList.size();++i){
				int result = isProjectionNodeRedundant((ProjectionNode)candList.get(i),(ProjectionNode)curNode);
				if(result != 0){
					sign = false;
					break;
				}
			}
			if(sign)
				candList.add(curNode);
			curNode = curNode.getChild(0);
		}
		if(candList.size()<1) return;
		ProjectionNode pNode = new ProjectionNode();
		pNode.setNodeID(queryTree.getNodeID());
		queryTree.setNodeID(queryTree.getNodeID()+1);
		pNode.setNodeName("Projection");
		pNode.setParent(null);
		pNode.setRoot();
		queryTree.setRoot(pNode);
		pNode.setSiteID(-1);
		for(int i=0;i<candList.size();++i){
			ProjectionNode p = (ProjectionNode)candList.get(i);
			pNode.addTableName(p.getTableNameList().get(0));
			pNode.addAttribute(p.getAttributeList().get(0));
		}
		curNode.getParent().removeChildNode(curNode);
		curNode.setParent(pNode);
	}
	
	private void mergeNode_from_bottom(){
		ArrayList<LeafNode> leafList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		for(int i=0;i<leafList.size();++i){
			LeafNode lNode = (LeafNode)leafList.get(i);
			mergeNode_from_bottom(lNode);
		}
	}
	private void mergeNode_from_bottom(LeafNode lNode){
		TreeNode curNode = lNode;
		ArrayList<TreeNode> candPNodeList = new ArrayList<TreeNode>();
		ArrayList<TreeNode> candSNodeList = new ArrayList<TreeNode>();
		while(curNode.getParent()!=null && curNode.getParent().getChildCount()==1){
			if(curNode.getParent() instanceof ProjectionNode){
				/*boolean sign= true;
				for(int i=0;i<candPNodeList.size();++i){
					int result = isProjectionNodeRedundant((ProjectionNode)candPNodeList.get(i),(ProjectionNode)curNode.getParent());
					if(result != 0){
						sign = false;
						if(result==2){
							candPNodeList.remove(i);
							candPNodeList.add(curNode.getParent());
						}
						break;
					}
				}
				if(sign)
				*/
					candPNodeList.add(curNode.getParent());
			}
			else if(curNode.getParent() instanceof SelectionNode)
				candSNodeList.add(curNode.getParent());
			curNode = curNode.getParent();
		}
		boolean hasRedudant = true;
		while(hasRedudant){
			hasRedudant = false;
			for(int i=0;i<candPNodeList.size()-1;++i){
				for(int j=i+1;j<candPNodeList.size();++j){
					int result = isProjectionNodeRedundant((ProjectionNode)candPNodeList.get(i),(ProjectionNode)candPNodeList.get(j));
					if(result != 0){
						hasRedudant = true;
						if(result == 1){
							candPNodeList.remove(j);
						}
						else{
							candPNodeList.remove(i);
						}
						break;
					}
				}
				if(hasRedudant)
					break;
			}
		}
		if(candPNodeList.size()< 1 && candSNodeList.size()<=1) return;
		
		if(candPNodeList.size()>= 1){
			ProjectionNode newPNode = new ProjectionNode();
			newPNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newPNode.setNodeName("Projection");
			newPNode.setSiteID(-1);
			newPNode.setParent(curNode.getParent());
			if(curNode.getParent()!=null)
				curNode.getParent().removeChildNode(curNode);
			else
				queryTree.setRoot(newPNode);
			curNode.setParent(null);
			for(int i=0;i<candPNodeList.size();++i){
				ProjectionNode tmp = (ProjectionNode)candPNodeList.get(i);
				newPNode.addTableName(tmp.getTableNameList().get(0));
				newPNode.addAttribute(tmp.getAttributeList().get(0));
				if(tmp.getParent()!=null)
					tmp.getParent().removeChildNode(tmp);
			}
			curNode = newPNode;
		}
	
		if (candSNodeList.size() == 1){
			candSNodeList.get(0).setParent(curNode);
			curNode = candSNodeList.get(0);
		}
		else if(candSNodeList.size()>1){
			SelectionNode newSNode = new SelectionNode();
			newSNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newSNode.setNodeName("Selection");
			newSNode.setParent(curNode);
			newSNode.setSiteID(-1);
			for(int i=0;i<candSNodeList.size();++i){
				SelectionNode tmp = (SelectionNode)candSNodeList.get(i);
				newSNode.addConditon(tmp.getCondList().get(0));
				newSNode.setTableName(tmp.getTableName());	
				if(tmp.getParent()!=null)
					tmp.getParent().removeChildNode(tmp);
			}
			curNode = newSNode;
			lNode.setParent(curNode);
		}
		else{
			lNode.setParent(curNode);
		}
	}
	private int isProjectionNodeRedundant(ProjectionNode p1,ProjectionNode p2){
		if(p1.getTableNameList().size()!=1||p1.getTableNameList().size()!=1) 
			return 0;
		if(p1.getAttributeList().size()!=1||p2.getAttributeList().size()!=1)
			return 0;
		String tName1 = p1.getTableNameList().get(0);
		String tName2 = p2.getTableNameList().get(0);
		//System.out.println("PPPPPPPPP "+tName1+"."+p1.getAttributeList().get(0)+" "+tName2+"."+p2.getAttributeList().get(0));
		if(tName1.startsWith(tName2)||tName2.startsWith(tName1)){
			if(p1.getAttributeList().get(0).equalsIgnoreCase(p2.getAttributeList().get(0))||p1.getAttributeList().get(0).equalsIgnoreCase("*"))
				return 1;
			else if(p2.getAttributeList().get(0).equalsIgnoreCase("*"))
				return 2;
		}
		return 0;
	}
	private boolean isConditionConfilct(SimpleExpression exp1,SimpleExpression exp2)
	{
		//exp1 as fragment condition,exp2 as selection condition
		//gdd.getTableInfoFromFragName(exp1.tableName).getTableName()
		String suTableName1 = exp1.tableName;
		if(gdd.getTableInfoFromFragName(exp1.tableName)!=null)
			suTableName1 = gdd.getTableInfoFromFragName(exp1.tableName).getTableName();
		
		String suTableName2 = exp2.tableName;
		if(gdd.getTableInfoFromFragName(exp2.tableName)!=null)
			suTableName2 = gdd.getTableInfoFromFragName(exp2.tableName).getTableName();

		if(!(suTableName1!=null && suTableName1.equalsIgnoreCase(exp2.tableName))){
			if(!(suTableName1!=null && suTableName2!=null && suTableName1.equalsIgnoreCase(suTableName2)))	
				return false;
		}
		if(!exp1.columnName.equalsIgnoreCase(exp2.columnName))
			return false;
		if(exp1.valueType !=exp2.valueType)
		{
			System.out.println("Data Type Error: left type ="+exp1.valueType+" and right type="+exp2.valueType);
			return false;
		}
		if(exp1.valueType == CONSTANT.VALUE_STRING && exp1.value.startsWith("'")&&exp1.value.endsWith("'"))
			exp1.value = exp1.value.substring(1,exp1.value.length()-1);
		if(exp2.valueType == CONSTANT.VALUE_STRING && exp2.value.startsWith("'")&&exp2.value.endsWith("'"))
			exp2.value = exp2.value.substring(1,exp2.value.length()-1);
		if(exp1.op.equalsIgnoreCase("="))
		{
			if(exp1.valueType == CONSTANT.VALUE_STRING)
			{
				if(exp2.op.equalsIgnoreCase("=")){
					if(!exp1.value.equalsIgnoreCase(exp2.value))
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
					if(exp1.value.equalsIgnoreCase(exp2.value))
						return true;
				}
			}
			else
			{
				int v1 = Integer.parseInt(exp1.value);
				int v2 = Integer.parseInt(exp2.value);
				if(exp2.op.equalsIgnoreCase("=")){
					if(v1 != v2)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
					if(v1 == v2)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase(">=")){
					if(v2>v1)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase(">")){
					if(v2>=v1)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<=")){
					if(v2<v1)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<")){
					if(v2<=v1)
						return true;
				}
			}
		}
		else if(exp1.op.equalsIgnoreCase(">"))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase("=")){
				if(v1 >= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<=")||exp2.op.equalsIgnoreCase("<")){
				if(v2<=v1)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase("<"))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase("=")){
				if(v1 <= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase(">=")||exp2.op.equalsIgnoreCase(">")){
				if(v1<=v2)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase(">="))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase("=")){
				if(v1 > v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<=")){
				if(v2<v1)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<")){
				if(v2<=v1)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase("<="))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase("=")){
				if(v1 < v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase(">=")){
				if(v2>v1)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase(">")){
				if(v2>=v1)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase("<>")||exp1.op.equalsIgnoreCase("!="))
		{
			if(exp1.valueType == CONSTANT.VALUE_STRING)
			{
				if(exp2.op.equalsIgnoreCase("=")){
					if(exp1.value.equalsIgnoreCase(exp2.value))
						return true;
				}
			}
			else
			{
				int v1 = Integer.parseInt(exp1.value);
				int v2 = Integer.parseInt(exp2.value);
				if(exp2.op.equalsIgnoreCase("=")){
					if(v1 == v2)
						return true;
				}
			}
		}
		return false;
	}
	private boolean isConditionRedundant(SimpleExpression exp1,SimpleExpression exp2)
	{
		//exp1 as fragment condition,exp2 as selection condition
		//gdd.getTableInfoFromFragName(exp1.tableName).getTableName()
		String suTableName1 = exp1.tableName;
		if(gdd.getTableInfoFromFragName(exp1.tableName)!=null)
			suTableName1 = gdd.getTableInfoFromFragName(exp1.tableName).getTableName();
		
		String suTableName2 = exp2.tableName;
		if(gdd.getTableInfoFromFragName(exp2.tableName)!=null)
			suTableName2 = gdd.getTableInfoFromFragName(exp2.tableName).getTableName();

		if(!(suTableName1!=null && suTableName1.equalsIgnoreCase(exp2.tableName))){
			if(!(suTableName1!=null && suTableName2!=null && suTableName1.equalsIgnoreCase(suTableName2)))	
				return false;
		}
		if(!exp1.columnName.equalsIgnoreCase(exp2.columnName))
			return false;
		if(exp1.valueType !=exp2.valueType)
		{
			System.out.println("Data Type Error: left type ="+exp1.valueType+" and right type="+exp2.valueType);
			return false;
		}
		if(exp1.valueType == CONSTANT.VALUE_STRING && exp1.value.startsWith("'")&&exp1.value.endsWith("'"))
			exp1.value = exp1.value.substring(1,exp1.value.length()-1);
		if(exp2.valueType == CONSTANT.VALUE_STRING && exp2.value.startsWith("'")&&exp2.value.endsWith("'"))
			exp2.value = exp2.value.substring(1,exp2.value.length()-1);
		if(exp1.op.equalsIgnoreCase("="))
		{
			if(exp1.valueType == CONSTANT.VALUE_STRING)
			{
				if(exp2.op.equalsIgnoreCase("=")){
					if(exp1.value.equalsIgnoreCase(exp2.value))
						return true;
				}
			}
			else
			{
				int v1 = Integer.parseInt(exp1.value);
				int v2 = Integer.parseInt(exp2.value);
				if(exp2.op.equalsIgnoreCase("=")){
					if(v1 == v2)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
					if(v1 != v2)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase(">=")){
					if(v2<=v1)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase(">")){
					if(v2<v1)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<=")){
					if(v2>=v1)
						return true;
				}
				else if(exp2.op.equalsIgnoreCase("<")){
					if(v2>v1)
						return true;
				}
			}
		}
		else if(exp1.op.equalsIgnoreCase(">"))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase(">")){
				if(v1 >= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase(">=")){
				if(v1 >= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
				if (v1 >= v2)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase("<"))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase("<")){
				if(v1 <= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<=")){
				if(v1 <= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
				if( v1 <= v2)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase(">="))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase(">")){
				if(v1 > v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase(">=")){
				if(v1 >= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
				if(v1 > v2)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase("<="))
		{
			int v1 = Integer.parseInt(exp1.value);
			int v2 = Integer.parseInt(exp2.value);
			if(exp2.op.equalsIgnoreCase("<")){
				if(v1 < v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<=")){
				if( v1 <= v2)
					return true;
			}
			else if(exp2.op.equalsIgnoreCase("<>")||exp2.op.equalsIgnoreCase("!=")){
				if( v1 < v2)
					return true;
			}
		}
		else if(exp1.op.equalsIgnoreCase("<>")||exp1.op.equalsIgnoreCase("!="))
		{
			if(exp1.valueType == CONSTANT.VALUE_STRING)
			{
				if(exp2.op.equalsIgnoreCase("!=")||exp2.op.equalsIgnoreCase("<>")){
					if(exp1.value.equalsIgnoreCase(exp2.value))
						return true;
				}
			}
			else
			{
				int v1 = Integer.parseInt(exp1.value);
				int v2 = Integer.parseInt(exp2.value);
				if(exp2.op.equalsIgnoreCase("!=")||exp2.op.equalsIgnoreCase("<>")){
					if(v1 == v2)
						return true;
				}
			}
		}
		return false;
	}
	private void siteDeployment(){
		if( queryTree.getRoot() == null) return;
		siteDeployment(queryTree.getRoot());
	}
	private int siteDeployment(TreeNode node){
		if(node.isLeaf()) {
			return node.getSiteID();
		}
		List<TreeNode> childList = node.getChildList();
		for(int i=0;i<childList.size();++i)
		{
			siteDeployment(childList.get(i));
		}
		if(node instanceof UnionNode){
			UnionNode uNode = (UnionNode)node;
			int max = -1;
			int idx = -1;
			for(int i=0;i<uNode.getChildCount();++i){
				int tmp = getMaxFragSize(uNode.getChild(i));
				if(max==-1||max<tmp){
					max = tmp;
					idx = i;
				}
			}
			node.setSiteID(siteDeployment(node.getChild(idx)));
		}
		else if(node instanceof JoinNode){
			JoinNode jNode = (JoinNode)node;
			int max = -1;
			int idx = -1;
			for(int i=0;i<jNode.getChildCount();++i){
				int tmp = getMaxFragSize(jNode.getChild(i));
				if(max==-1||max<tmp){
					max = tmp;
					idx = i;
				}
			}
			node.setSiteID(siteDeployment(node.getChild(idx)));
		}
		else{
			node.setSiteID(siteDeployment(node.getChild(0)));
		}
		return node.getSiteID();
	}
	private int getMaxFragSize(TreeNode node){
		if(node.isLeaf()) 
			return gdd.getFragmentation(((LeafNode)node).getTableName()).getFragSize();
		int max = -1;
		for(int i=0;i<node.getChildCount();++i){
			int tmp = getMaxFragSize(node.getChild(i));
			if(max == -1 || max<tmp){
				max = tmp;
			}
		}
		return max;
	}
	
	private TreeNode copyTree(TreeNode oldNode){
		TreeNode newNode = copyNode(oldNode);
		if(oldNode instanceof LeafNode){
			return newNode;
		}
		else{
			copyTree(oldNode,newNode);
			return newNode;
		}
	}
	private TreeNode copyNode(TreeNode oldNode){
		if(oldNode instanceof LeafNode){
			LeafNode lNode = (LeafNode)oldNode;
			LeafNode newNode = new LeafNode();
			newNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newNode.setNodeName(lNode.getNodeName());
			newNode.setTableName(lNode.getTableName());
			newNode.setSegment(lNode.hasSegmented());
			newNode.setSiteID(lNode.getSiteID());
			return newNode;
		}
		else if(oldNode instanceof ProjectionNode){
			ProjectionNode pNode = (ProjectionNode)oldNode;
			ProjectionNode newNode = new ProjectionNode();
			newNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newNode.setNodeName(pNode.getNodeName());
			newNode.setTableNameList(pNode.getTableNameList());
			for(int i=0;i<pNode.getAttributeNum();++i){
				newNode.addAttribute(pNode.getAttributeList().get(i));
			}
			newNode.setSiteID(pNode.getSiteID());
			return newNode;
		}
		else if(oldNode instanceof SelectionNode){
			SelectionNode sNode = (SelectionNode) oldNode;
			SelectionNode newNode = new SelectionNode();
			newNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newNode.setNodeName(sNode.getNodeName());
			newNode.setSiteID(sNode.getSiteID());
			newNode.setTableName(sNode.getTableName());
			newNode.setCondList(sNode.getCondList());
			return newNode;
		}
		else if(oldNode instanceof JoinNode){
			JoinNode jNode = (JoinNode)oldNode;
			JoinNode newNode = new JoinNode();
			newNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newNode.setLeftTableName(jNode.getLeftTableName());
			newNode.setNodeName(jNode.getNodeName());
			newNode.setRightTableName(jNode.getRightTableName());
			newNode.setSiteID(jNode.getSiteID());
			if(jNode.getAttributeList().size()>0){
				for (Iterator<String> i = jNode.getAttributeList().keySet().iterator(); i.hasNext();) { 
					String key = i.next(); 
					String value = jNode.getAttributeList().get(key); 
					newNode.addAttribute(key,value);
				}
			}
			return newNode;
		}
		else{
			UnionNode uNode = (UnionNode)oldNode;
			UnionNode newNode = new UnionNode();
			newNode.setNodeID(queryTree.getNodeID());
			queryTree.setNodeID(queryTree.getNodeID()+1);
			newNode.setNodeName(uNode.getNodeName());
			newNode.setSiteID(uNode.getSiteID());
			return newNode;
		}
	}
	private void copyTree(TreeNode oldNode,TreeNode newNode){
		if(oldNode instanceof LeafNode)
			return;
		for(int i=0;i<oldNode.getChildCount();++i){
			TreeNode child = oldNode.getChild(i);
			TreeNode newChild = copyNode(child);
			newChild.setParent(newNode);
			copyTree(child,newChild);
		}
	}
	public void setTreeOptimize(boolean treeOptimize) {
		this.treeOptimize = treeOptimize;
	}

	public boolean isTreeOptimize() {
		return treeOptimize;
	}
	private void adapt_join_node(JoinNode jNode){
		if(jNode.getChildCount()!=2) return;
		TreeNode curNode = jNode.getChild(0);
		if(curNode instanceof JoinNode){
			JoinNode nJNode = (JoinNode)curNode;
			String lTableName = nJNode.getLeftTableName();
			String rTableName = nJNode.getRightTableName();
			String superTableName = jNode.getLeftTableName();
			if(lTableName!=null && (superTableName.equalsIgnoreCase(lTableName)||superTableName.startsWith(lTableName)||lTableName.startsWith(superTableName)))
				return;
			if(rTableName!=null && (superTableName.equalsIgnoreCase(rTableName)||superTableName.startsWith(rTableName)||rTableName.startsWith(superTableName)))
				return;
			jNode.setLeftTableName(jNode.getRightTableName());
			jNode.setRightTableName(superTableName);
			Iterator<String> it = jNode.getAttributeList().keySet().iterator(); 
			String key = it.next();
			String value = jNode.getAttributeList().get(key);
			jNode.getAttributeList().remove(key);
			jNode.addAttribute(value, key);
		}
		else if(curNode instanceof SelectionNode){
			SelectionNode sNode = (SelectionNode)curNode;
			String tableName = sNode.getTableName();
			String superTableName = jNode.getLeftTableName();
			if(superTableName.equalsIgnoreCase(tableName)||superTableName.startsWith(tableName)||tableName.startsWith(superTableName))
				return;
			jNode.setLeftTableName(jNode.getRightTableName());
			jNode.setRightTableName(superTableName);
			Iterator<String> it = jNode.getAttributeList().keySet().iterator(); 
			String key = it.next();
			String value = jNode.getAttributeList().get(key);
			jNode.getAttributeList().remove(key);
			jNode.addAttribute(value, key);
		}
		else if(curNode instanceof ProjectionNode){
			ProjectionNode pNode = (ProjectionNode)curNode;
			String tableName = pNode.getTableNameList().get(0);
			String superTableName = jNode.getLeftTableName();
			if(superTableName.equalsIgnoreCase(tableName)||superTableName.startsWith(tableName)||tableName.startsWith(superTableName))
				return;
			jNode.setLeftTableName(jNode.getRightTableName());
			jNode.setRightTableName(superTableName);
			Iterator<String> it = jNode.getAttributeList().keySet().iterator(); 
			String key = it.next();
			String value = jNode.getAttributeList().get(key);
			jNode.getAttributeList().remove(key);
			jNode.addAttribute(value, key);
		}
		else if(curNode instanceof UnionNode){
			UnionNode uNode = (UnionNode)curNode;
			TreeNode tmpNode = uNode.getChild(0);
			while(!tmpNode.isLeaf()) tmpNode = tmpNode.getChild(0);
			String tableName = ((LeafNode)tmpNode).getTableName();
			String superTableName = jNode.getLeftTableName();
			if(superTableName.equalsIgnoreCase(tableName)||superTableName.startsWith(tableName)||tableName.startsWith(superTableName))
				return;
			jNode.setLeftTableName(jNode.getRightTableName());
			jNode.setRightTableName(superTableName);
			Iterator<String> it = jNode.getAttributeList().keySet().iterator(); 
			String key = it.next();
			String value = jNode.getAttributeList().get(key);
			jNode.getAttributeList().remove(key);
			jNode.addAttribute(value, key);
		}
		else{
			LeafNode lNode = (LeafNode)curNode;
			String tableName = lNode.getTableName();
			String superTableName = jNode.getLeftTableName();
			if(superTableName.equalsIgnoreCase(tableName)||superTableName.startsWith(tableName)||tableName.startsWith(superTableName))
				return;
			jNode.setLeftTableName(jNode.getRightTableName());
			jNode.setRightTableName(superTableName);
			Iterator<String> it = jNode.getAttributeList().keySet().iterator(); 
			String key = it.next();
			String value = jNode.getAttributeList().get(key);
			jNode.getAttributeList().remove(key);
			jNode.addAttribute(value, key);
		}
		
	}
	private void adapt_join_node(){
		ArrayList<LeafNode> leafList = (ArrayList<LeafNode>) queryTree.getLeafNodeList();
		for(int i=0;i<leafList.size();++i){
			TreeNode node = leafList.get(i);
			while(node!=null){
				if(node instanceof JoinNode)
					adapt_join_node((JoinNode)node);
				node = node.getParent();
			}
		}
	}
	private void node_classification(){
		if(queryTree.getRoot() == null) return;
		gen_node_list(queryTree.getRoot());
		System.out.println("join nodes: "+joinList.size());
		System.out.println("projection nodes: "+projectionList.size());
		System.out.println("union nodes: "+unionList.size());
		System.out.println("selection nodes: "+selectionList.size());
		System.out.println("leaf nodes: "+leafList.size());
		//queryTree.getRoot().setClassID(queryTree.getClassID());
		//queryTree.setClassID(queryTree.getClassID()+1);
		if(projectionList.size()>0){
			projectionList.get(0).setClassID(queryTree.getClassID());
			queryTree.setClassID(queryTree.getClassID()+1);
			for(int i=1;i<projectionList.size();++i){
				for(int j=0;j<i-1;++j){
					if(is_in_same_classfication(projectionList.get(i),projectionList.get(j))){
						projectionList.get(i).setClassID(projectionList.get(j).getClassID());
					}
				}
				if(projectionList.get(i).getClassID()<0){
					projectionList.get(i).setClassID(queryTree.getClassID());
					queryTree.setClassID(queryTree.getClassID()+1);
				}
			}
		}
		if(joinList.size()>0){
			joinList.get(0).setClassID(queryTree.getClassID());
			queryTree.setClassID(queryTree.getClassID()+1);
			for(int i=1;i<joinList.size();++i){
				for(int j=0;j<i-1;++j){
					if(is_in_same_classfication(joinList.get(i),joinList.get(j))){
						joinList.get(i).setClassID(joinList.get(j).getClassID());
					}
				}
				if(joinList.get(i).getClassID()<0){
					joinList.get(i).setClassID(queryTree.getClassID());
					queryTree.setClassID(queryTree.getClassID()+1);
				}
			}
		}
		if(unionList.size()>0){
			unionList.get(0).setClassID(queryTree.getClassID());
			queryTree.setClassID(queryTree.getClassID()+1);
			for(int i=1;i<unionList.size();++i){
				for(int j=0;j<i-1;++j){
					if(is_in_same_classfication(unionList.get(i),unionList.get(j))){
						unionList.get(i).setClassID(unionList.get(j).getClassID());
					}
				}
				if(unionList.get(i).getClassID()<0){
					unionList.get(i).setClassID(queryTree.getClassID());
					queryTree.setClassID(queryTree.getClassID()+1);
				}
			}
		}
		if(selectionList.size()>0){
			selectionList.get(0).setClassID(queryTree.getClassID());
			queryTree.setClassID(queryTree.getClassID()+1);
			for(int i=1;i<selectionList.size();++i){
				for(int j=0;j<i-1;++j){
					if(is_in_same_classfication(selectionList.get(i),selectionList.get(j))){
						selectionList.get(i).setClassID(selectionList.get(j).getClassID());
					}
				}
				if(selectionList.get(i).getClassID()<0){
					selectionList.get(i).setClassID(queryTree.getClassID());
					queryTree.setClassID(queryTree.getClassID()+1);
				}
			}
		}
		if(leafList.size()>0){
			leafList.get(0).setClassID(queryTree.getClassID());
			queryTree.setClassID(queryTree.getClassID()+1);
			for(int i=1;i<leafList.size();++i){
				for(int j=0;j<i-1;++j){
					if(is_in_same_classfication(leafList.get(i),leafList.get(j))){
						leafList.get(i).setClassID(leafList.get(j).getClassID());
					}
				}
				if(leafList.get(i).getClassID()<0){
					leafList.get(i).setClassID(queryTree.getClassID());
					queryTree.setClassID(queryTree.getClassID()+1);
				}
			}
		}
		System.out.println("classfication size="+queryTree.getClassficationSize());
		union_node_add_key();
		joinList.clear();
		projectionList.clear();
		unionList.clear();
		leafList.clear();
		selectionList.clear();
	}
	private void gen_node_list(TreeNode node){
		if(node.isLeaf()){
			leafList.add((LeafNode)node);
			return;
		}
		if(node instanceof JoinNode){
			joinList.add((JoinNode)node);
			for(int i=0;i<node.getChildCount();++i)
				gen_node_list(node.getChild(i));
			return;
		}
		if(node instanceof ProjectionNode){
			projectionList.add((ProjectionNode)node);
			for(int i=0;i<node.getChildCount();++i)
				gen_node_list(node.getChild(i));
			return;
		}
		if(node instanceof SelectionNode){
			selectionList.add((SelectionNode)node);
			for(int i=0;i<node.getChildCount();++i)
				gen_node_list(node.getChild(i));
			return;
		}
		if(node instanceof UnionNode){
			unionList.add((UnionNode)node);
			for(int i=0;i<node.getChildCount();++i)
				gen_node_list(node.getChild(i));
			return;
		}
	}
	private boolean is_in_same_classfication(TreeNode node1,TreeNode node2){
		if(!node1.getNodeType().equalsIgnoreCase(node2.getNodeType()))
			return false;
		if(node1 instanceof LeafNode){
			LeafNode lNode1 = (LeafNode)node1;
			LeafNode lNode2 = (LeafNode)node2;
			if(!lNode1.getTableName().equalsIgnoreCase(lNode2.getTableName()))
				return false;
			else 
				return true;
		}
		if(node1 instanceof JoinNode){
			JoinNode jNode1 = (JoinNode)node1;
			JoinNode jNode2 = (JoinNode)node2;
			if(!jNode1.getLeftTableName().equalsIgnoreCase(jNode2.getLeftTableName()))
					return false;
			if(!jNode1.getRightTableName().equalsIgnoreCase(jNode2.getRightTableName()))
					return false;
			Iterator<String> it1 = jNode1.getAttributeList().keySet().iterator(); 
			String key1 = it1.next();
			String value1 = jNode1.getAttributeList().get(key1);
			Iterator<String> it2 = jNode2.getAttributeList().keySet().iterator(); 
			String key2 = it2.next();
			String value2 = jNode2.getAttributeList().get(key2);
			if(!key1.equalsIgnoreCase(key2))
				return false;
			if(!value1.equalsIgnoreCase(value2))
				return false;
			if(jNode1.getChildCount()!=jNode2.getChildCount())
				return false;
			for(int i=0;i<jNode1.getChildCount();++i){
				if(!is_in_same_classfication(jNode1.getChild(i),jNode2.getChild(i)))
					return false;
			}
			return true;
		}
		if(node1 instanceof SelectionNode){
			SelectionNode sNode1 = (SelectionNode)node1;
			SelectionNode sNode2 = (SelectionNode)node2;
			if(!sNode1.getTableName().equalsIgnoreCase(sNode2.getTableName()))
				return false;
			if(sNode1.getCondList().size()!=sNode2.getCondList().size())
				return false;
			for(int i=0;i<sNode1.getCondList().size();++i){
				if(!sNode1.getCondList().get(i).equals(sNode2.getCondList().get(i)))
					return false;
			}
			if(sNode1.getChildCount()!=sNode2.getChildCount())
				return false;
			for(int i=0;i<sNode1.getChildCount();++i){
				if(!is_in_same_classfication(sNode1.getChild(i),sNode2.getChild(i)))
					return false;
			}
			return true;
		}
		if(node1 instanceof ProjectionNode){
			ProjectionNode pNode1 = (ProjectionNode)node1;
			ProjectionNode pNode2 = (ProjectionNode)node2;
			if(pNode1.getTableNameList().size()!=pNode2.getTableNameList().size())
				return false;
			for(int i=0;i<pNode1.getTableNameList().size();++i){
				if(!pNode1.getTableNameList().get(i).equalsIgnoreCase(pNode2.getTableNameList().get(i)))
					return false;
			}
			if(pNode1.getAttributeNum()!=pNode2.getAttributeNum())
				return false;
			for(int i=0;i<pNode1.getAttributeNum();++i){
				if(!pNode1.getAttributeList().get(i).equalsIgnoreCase(pNode2.getAttributeList().get(i)))
					return false;
			}
			if(pNode1.getChildCount()!=pNode2.getChildCount())
				return false;
			for(int i=0;i<pNode1.getChildCount();++i){
				if(!is_in_same_classfication(pNode1.getChild(i),pNode2.getChild(i)))
					return false;
			}
			return true;
		}
		//UnionNode
		UnionNode uNode1 = (UnionNode)node1;
		UnionNode uNode2 = (UnionNode)node2;
		if(uNode1.getChildCount()!=uNode2.getChildCount())
			return false;
		for(int i=0;i<uNode1.getChildCount();++i){
			if(!is_in_same_classfication(uNode1.getChild(i),uNode2.getChild(i)))
				return false;
		}
		return true;
	}
	private void union_node_add_key(){
		if(unionList.size()==0) return;
		for(int i=1;i<unionList.size();++i){
			if(isNodeKeyAddable(unionList.get(i))){
				unionList.get(i).setAddKey(true);
			}
		}
	}
	private boolean isNodeKeyAddable(TreeNode node){
		if(node instanceof UnionNode){
			UnionNode uNode = (UnionNode)node;
			for(int i=0;i<uNode.getChildCount();++i){
				if(!uNode.getChild(i).isLeaf())
					return false;
			}
			return true;
		}
		return false;
	}
	private boolean isAllChildNode_LeafNode(TreeNode node){
		if(node.isLeaf())
			return false;
		if(node.getChildCount()==0)
			return false;
		for(int i=0;i<node.getChildCount();++i){
			if(!node.getChild(i).isLeaf())
				return false;
		}
		return true;
	}
	private boolean isNoJoinNodeBellow(TreeNode node){
		if(node.isLeaf())
			return true;
		if(node instanceof JoinNode)
			return false;
		if(node.getChildCount()==0)
			return true;
		for(int i=0;i<node.getChildCount();++i){
			if(!isNoJoinNodeBellow(node.getChild(i)))
				return false;
		}
		return true;
	}
	private String getMinimalTableName(){
		int size = -1;
		String tableName = "";
		for(int i=0;i<gdd.getTableInfos().size();++i)
		{
			int tmp_size = gdd.getTableSize(gdd.getTableInfos().get(i).getTableName());
			if(size==-1||size>tmp_size){
				size = tmp_size;
				tableName = gdd.getTableInfos().get(i).getTableName();
			}
		}
		return tableName;
	}
}
