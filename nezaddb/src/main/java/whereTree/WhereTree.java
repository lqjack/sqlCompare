package whereTree;

public class WhereTree {
	private WhereNode root = null;
	public WhereTree(){
	}
	public WhereNode getRoot(){
		return root;
	}
	public void setRoot(WhereNode r){
		root = r;
	}
	public void displayTree(){
		if (root == null) return;
		display(root);
	}
	private void display(WhereNode node){
		if(node.IsLeaf()){
			node.display();
		}
		else{
			display(node.getLeftChild());
			node.display();
			display(node.getRightChild());
		}
	}

}
