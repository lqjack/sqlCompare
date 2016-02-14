package executeReturnResult;

import java.util.List;

import queryTree.FormattedTreeNode;

public class ParseTreeReturnResult extends ExecuteReturnResult {
    static final long serialVersionUID = 1;
    private List<FormattedTreeNode> nodeList = null;
    
    public ParseTreeReturnResult(List<FormattedTreeNode> list) {
        nodeList = list;
    }
    
    public List<FormattedTreeNode> getTreeList() {
        return nodeList;
    }
    
    @Override
    public void displayResult() {
        // TODO Auto-generated method stub

    }

}
