package executeReturnResult;

import globalDefinition.CONSTANT;

import java.util.Vector;

public class DeleteReturnResult extends ExecuteReturnResult{
    static final long serialVersionUID = 1;
	Vector<String> returninfos;
	
	
	public DeleteReturnResult(){
		this.setExecuteType(CONSTANT.SQL_DELETE);
		this.returninfos = new Vector<String>();
	}
	
	public void addinfo(String s){
		if(this.returninfos == null)
			this.returninfos = new Vector<String>();
		this.returninfos.add(s);
	}
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		for(int i = 0 ; i < this.returninfos.size(); i++){
			System.out.println(this.returninfos.elementAt(i));
		}
	}

}
