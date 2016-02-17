package queryTree;

import java.util.ArrayList;
import java.util.List;

import globalDefinition.SimpleExpression;

public class SelectionNode extends TreeNode{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tableName;
	private List<SimpleExpression> condList = null;
	@Override
	public void accept(TreeNodeVisitor visitor) {
		
		visitor.visit(this);
	}

	@Override
	public boolean isLeaf() {
		
		return false;
	}

	@Override
	public String getNodeType() {
		
		return "Selection";
	}

	@Override
	public String getContent() {
		
		String cnt = new String();
		cnt += "Selection: ";
		for(int i=0;i<condList.size();++i){
			if(condList.get(i).op.equalsIgnoreCase("<"))
				cnt += condList.get(i).tableName+"."+condList.get(i).columnName+" Less Than "+condList.get(i).value;
			else if(condList.get(i).op.equalsIgnoreCase("<="))
				cnt += condList.get(i).tableName+"."+condList.get(i).columnName+" LessEqual Than "+condList.get(i).value;
			else
				cnt += condList.get(i).tableName+"."+condList.get(i).columnName+condList.get(i).op+condList.get(i).value;
			if(i<condList.size()-1)
				cnt +=" and ";
		}
		return cnt;
	}

	public void setCondList(List<SimpleExpression> condList) {
		this.condList = condList;
	}

	public List<SimpleExpression> getCondList() {
		return condList;
	}
	public void addConditon(SimpleExpression e){
		if(condList == null){
			condList = new ArrayList<SimpleExpression>();
		}
		condList.add(e);
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}
}
