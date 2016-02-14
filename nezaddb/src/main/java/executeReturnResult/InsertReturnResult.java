package executeReturnResult;

import java.util.Vector;

import globalDefinition.CONSTANT;

public class InsertReturnResult extends ExecuteReturnResult{
    static final long serialVersionUID = 1;
	Vector<String> returninfos;
	
	public InsertReturnResult(){
		this.setExecuteType(CONSTANT.SQL_INSERT);
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
