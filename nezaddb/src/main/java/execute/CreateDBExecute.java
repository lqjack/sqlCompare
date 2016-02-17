package execute;

import java.io.IOException;
import java.util.Vector;

import communication.ClientBase;

import executeResult.CreateDBExecuteResult;
import gdd.GDD;
import gdd.SiteMeta;
import parserResult.CreateDatabaseResult;
import parserResult.ParseResult;

public class CreateDBExecute extends ExecuteSQL{
	private GDD gdd;
	
	public CreateDBExecute(){
		gdd = GDD.getInstance();
	}

	@Override
	public void execute(ParseResult result) {
		
		CreateDatabaseResult createDatabaseResult = (CreateDatabaseResult)result;
		gdd.setDBName(createDatabaseResult.getDatabaseName());
		
		try{
			allocateTaskToSites();
		}catch(IOException ex){
			System.out.println(ex.toString());
		}
	}
	
	public boolean allocateTaskToSites()throws IOException{
		boolean flag = true;
		
		String createDBSql = "create database " + gdd.getDBName() + ";";
		
		
		Vector<SiteMeta> siteinfos  = gdd.getSiteInfo();
		for(int i = 0 ; i < siteinfos.size() ; i++){
			SiteMeta siteinfo = siteinfos.elementAt(i);
			
			CreateDBExecuteResult dbCreate = new CreateDBExecuteResult();
			dbCreate.setSql(createDBSql);
			
	        ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
	        Object result = client.sendContext("createdb", dbCreate);
	        
	        if(result == null){
	        	System.out.println("reuslt is null!");
	        }
	        
	        boolean r = ((Boolean)result).booleanValue();
	        if (r == true) {
	            System.out.println("success!");
	        }
	        flag &= r;
		}
		return flag;	
	}
	
	
}
