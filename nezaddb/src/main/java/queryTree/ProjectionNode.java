package queryTree;

import java.util.ArrayList;

public class ProjectionNode extends TreeNode{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<String> attributeList = new ArrayList<String>();
	private ArrayList<String> tableNameList = new ArrayList<String>();
	@Override
	public void accept(TreeNodeVisitor visitor) {
		
		visitor.visit(this);
	}

	@Override
	public boolean isLeaf() {
		
		return false;
	}
	
	public void addAttribute(String attr){
		attributeList.add(attr);
	}
	
	public void addTableName(String name){
		tableNameList.add(name);
	}
	public int getAttributeNum(){
		return attributeList.size();
	}
	
	public ArrayList<String> getAttributeList(){
		return attributeList;
	}
	@Override
	public String getNodeType() {
		
		return "Projection";
	}

	@Override
	public String getContent() {
		
		String cnt = new String();
		cnt += "Projection: ";
		for(int i=0;i<attributeList.size();++i){
			cnt += tableNameList.get(i)+"."+attributeList.get(i);
			if(i!= attributeList.size()-1)
				cnt+=", ";
		}
		return cnt;
	}

	public void setTableNameList(ArrayList<String> tableNameList) {
		this.tableNameList = tableNameList;
	}

	public ArrayList<String> getTableNameList() {
		return tableNameList;
	}

}
