package parserResult;

import queryTree.QueryTree;

public class InsertResult extends ParseResult{
	private QueryTree insertTree;
	public InsertResult(QueryTree tree){
		insertTree = tree;
	}
	@Override
	public void accept(ParseResultVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println("Insert parse result:");
		insertTree.displayTree();
	}

	public void setInsertTree(QueryTree insertTree) {
		this.insertTree = insertTree;
	}

	public QueryTree getInsertTree() {
		return insertTree;
	}

}
