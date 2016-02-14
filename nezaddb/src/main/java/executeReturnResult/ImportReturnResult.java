package executeReturnResult;

import java.util.Vector;

import globalDefinition.CONSTANT;

public class ImportReturnResult extends ExecuteReturnResult{
    static final long serialVersionUID = 1;
	Vector<String> returninfos;
	
	public ImportReturnResult(){
		this.setExecuteType(CONSTANT.SQL_IMPORTDATA);
		this.returninfos = new Vector<String>();
	}
	
	
	public void addInfo(String s){
		if(this.returninfos == null)
			this.returninfos = new Vector<String>();
		
		this.returninfos.add(s);
	}
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		System.out.println(returninfos);
	}

}
