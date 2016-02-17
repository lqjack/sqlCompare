package executeResult;

import queryTree.TreeNode;
//import queryTree.QueryTree;

public class SelectExecuteResult extends ExecuteResult {
    private static final long serialVersionUID = 1L;
	//private QueryTree selectTree;
    private TreeNode root;
    private int classesCount;
    private long uid;
	
	public SelectExecuteResult(TreeNode node, int count, long uid){
		root = node;
		classesCount = count;
		this.uid = uid;
	}
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("select: " + root.getNodeName());
	}
	
	public TreeNode getRoot() {
	    return root;
	}
	
	public int getClassesCount() {
	    return classesCount;
	}
	
	public long getUID() {
	    return uid;
	}
}
