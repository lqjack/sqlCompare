package queryTree;

public class LeafNode extends TreeNode{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tableName;
	private boolean b_Segment = false;
	
	@Override
	public void accept(TreeNodeVisitor visitor) {
		
		visitor.visit(this);
	}

	@Override
	public boolean isLeaf() {
		
		return true;
	}
	
	public void setTableName(String name){
		tableName = name;
	}
	public String getTableName(){
		return tableName;
	}
	public boolean hasSegmented(){
		return b_Segment;
	}
	public void setSegment(boolean b){
		b_Segment = b;
	}

	@Override
	public String getNodeType() {
		
		return "Leaf";
	}

	@Override
	public String getContent() {
		
		return tableName;
	}

}
