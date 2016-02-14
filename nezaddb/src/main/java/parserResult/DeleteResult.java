package parserResult;

import queryTree.QueryTree;

public class DeleteResult extends ParseResult{
	private QueryTree deleteTree;
	public DeleteResult(QueryTree tree){
		deleteTree = tree;
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("Delete parse result:");
		deleteTree.displayTree();
	}

	public void setDeleteTree(QueryTree deleteTree) {
		this.deleteTree = deleteTree;
	}

	public QueryTree getDeleteTree() {
		return deleteTree;
	}

}
