package executeReturnResult;

import globalDefinition.CONSTANT;

import java.util.Vector;

public class CreateTableReturnResult extends ExecuteReturnResult{
    static final long serialVersionUID = 1;
	Vector<String> returninfos;
	
	public CreateTableReturnResult(){
		this.setExecuteType(CONSTANT.SQL_CREATE);
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
		System.out.println("create table return result info:");
		if(this.returninfos == null)
			return ;
		for(int i = 0 ; i < this.returninfos.size(); i++){
			System.out.println(this.returninfos.elementAt(i));
		}
	}

}
