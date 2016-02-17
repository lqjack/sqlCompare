package executeResult;

import globalDefinition.CONSTANT;

import java.util.Vector;

public class ImportExecuteResult extends ExecuteResult {
    private static final long serialVersionUID = 1L;
	private Vector<ImportExecuteResultUnit> importUnits;
	
	public ImportExecuteResult(){
		this.setExecuteType(CONSTANT.SQL_IMPORTDATA);
	}
	
	public void addImportExecuteResult(ImportExecuteResultUnit unit){
		if(this.importUnits == null)
			this.importUnits = new Vector<ImportExecuteResultUnit>();
		this.importUnits.add(unit);
	}
	
	public void setImportSize(int size){
		this.importUnits  = new Vector<ImportExecuteResultUnit>(size);
	}
	
	public Vector<ImportExecuteResultUnit> getImportUnits(){
		return this.importUnits;
	}
	
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		if(this.importUnits == null)
			return ;
		for(int i = 0 ; i < this.importUnits.size(); i++){
			this.importUnits.elementAt(i).displayResult();
		}
	}

}
