package queryTree;

import java.io.Serializable;

public class FormattedTreeNode implements Serializable{
    static final long serialVersionUID = 1;
	public String content;
	public int nodeID;
	public int parentNodeID;
	public int siteID;
	public FormattedTreeNode(){
		content = new String();
		nodeID = -1;
		parentNodeID = -1;
		siteID = -1;
	}
}