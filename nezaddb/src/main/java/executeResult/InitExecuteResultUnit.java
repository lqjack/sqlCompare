package executeResult;

import java.util.Vector;

public class InitExecuteResultUnit {
	public String siteName;
	public String createDBSql;
	public String usageDBSql;
	public Vector<String> createTableSql;
		
	public InitExecuteResultUnit(){
			this.siteName = null;
			this.createDBSql = null;
			this.usageDBSql = null;
			this.createTableSql = new Vector<String>();
	}
	
}
