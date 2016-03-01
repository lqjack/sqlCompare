package slaveExecuteResult;

import java.util.Vector;

public class DeleteBatchResult implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Vector<String> deleteSqls;
	
	public DeleteBatchResult(){
		this.deleteSqls = new Vector<String>();
	}
	
	public void addSql(String sql){
		if(deleteSqls == null){
			this.deleteSqls = new Vector<String>();
		}
		this.deleteSqls.add(sql);
	}
	
	public Vector<String> getSqls(){
		return this.deleteSqls;
	}
}
