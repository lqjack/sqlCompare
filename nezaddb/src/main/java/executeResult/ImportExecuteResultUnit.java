package executeResult;

import java.util.Vector;

public class ImportExecuteResultUnit implements java.io.Serializable{
    private static final long serialVersionUID = 1L;
	public String siteName;
	public String tableName;
	public String columnNameString;
	public Vector<String> columnNames;
	public Vector<String> columnInfos;
	
	public ImportExecuteResultUnit(){
		
	}
	
	public void displayResult(){
		System.out.println("siteName="+siteName);
		System.out.println("tableName="+tableName);
		System.out.println("columnNameString="+columnNameString);
		for(int i = 0 ; i < columnNames.size() ; i++){
			System.out.print(columnNames.elementAt(i)+" ");
		}
		System.out.println();
		
		for(int i = 0 ; i < columnInfos.size() ; i++)
			System.out.println(columnInfos.elementAt(i));
	}
	
}
