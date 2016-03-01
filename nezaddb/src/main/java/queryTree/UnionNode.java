package queryTree;

public class UnionNode extends TreeNode{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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
		
		return "Union";
	}

	@Override
	public String getContent() {
		
		return "Union";
	}
	
}
