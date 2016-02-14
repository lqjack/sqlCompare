package executeResult;

import java.util.Vector;

public class InitExecuteResult extends ExecuteResult implements java.io.Serializable{
    private static final long serialVersionUID = 1L;
	private Vector<InitExecuteResultUnit> initResultUnits;

	public InitExecuteResult(){
		this.initResultUnits =  new Vector<InitExecuteResultUnit>();
	}
	
	public Vector<InitExecuteResultUnit> getInitResultUnits(){
		return this.initResultUnits;
	}
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		for(int i = 0 ; i < initResultUnits.size() ; i++){
			InitExecuteResultUnit unit = this.initResultUnits.elementAt(i);
			System.out.println(unit.createDBSql);
			System.out.println(unit.usageDBSql);
			for(int j = 0 ; j < unit.createTableSql.size() ; j++)
				System.out.println(unit.createTableSql.elementAt(j));
			
		}
		
	}

}
