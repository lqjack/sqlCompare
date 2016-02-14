package execute;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import communication.ClientBase;

import executeResult.CreateTableExecuteResult;
import executeReturnResult.CreateTableReturnResult;

import gdd.ColumnInfo;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteInfo;
import gdd.TableInfo;
import globalDefinition.CONSTANT;
import parserResult.AllocateResult;
import parserResult.ParseResult;
import slaveExecuteResult.SlaveCreateTableResult;

public class AllocateExecute extends ExecuteSQL{
	
	private GDD gdd;
	private CreateTableReturnResult createTableReturnResult;
	
	public AllocateExecute(){
		gdd = GDD.getInstance();
	}
	
	
	public CreateTableReturnResult getResult(){
		return this.createTableReturnResult;
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		AllocateResult allocateResult = (AllocateResult)result;
		List<AllocateResult.SiteTableMap> siteTableMap = allocateResult.getSiteTableMap();
		int i,j;
		for(i = 0 ; i < siteTableMap.size() ; i++){
			AllocateResult.SiteTableMap siteTable = siteTableMap.get(i);
			SiteInfo siteinfo = gdd.getSiteInfo(siteTable.siteName);
			if(siteinfo == null){
				System.out.println("site "+siteTable.siteName+" is not exist!");
				return;
			}
			
			for(j = 0 ; j < siteTable.tables.size() ; j++){
				siteinfo.getSiteFragNames().add(siteTable.tables.get(j).toString());
				siteinfo.addSiteFragNum();
				FragmentationInfo fraginfo = gdd.getFragmentation(siteTable.tables.get(j).toString());
				if(fraginfo == null){
					gdd.printTableInfos();
					System.out.println("fragmentation "+ siteTable.tables.get(j).toString()+" is not exist!");
					return;
				}
				fraginfo.setSiteName(siteTable.siteName);
			}
			
		}
		
		try{
			allocateTaskToSites(siteTableMap);
		}catch(IOException ex){
			System.out.println(ex.toString());
		}
	}
	
	public boolean allocateTaskToSites(List<AllocateResult.SiteTableMap> siteTableMap)throws IOException{
		//boolean flag = true;
		int i;
		
		this.createTableReturnResult = new CreateTableReturnResult();
		for(i = 0 ; i < siteTableMap.size() ; i++){
			AllocateResult.SiteTableMap siteTable = siteTableMap.get(i);
			CreateTableExecuteResult createResult = genInitResult(siteTable);
			//createResult.displayResult();
			SiteInfo siteinfo = gdd.getSiteInfo(siteTable.siteName);
			ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
	        System.out.println("create table client create success ");
			Object result = client.sendContext("createtable", createResult);
	        
	        if(!(result instanceof SlaveCreateTableResult)){
	        	System.out.println("error obect");
	        	return false;
	        }
	        
	        
	        SlaveCreateTableResult slaveCreateResult = (SlaveCreateTableResult)result;
	        String s;
	        if(slaveCreateResult.getSuccess()){
	        	s = "create table in site " + i+" successful!";
	        }else{
	        	s = slaveCreateResult.getErrorInfo();
	        }
	        this.createTableReturnResult.addinfo(s);    
		}
		return true;
	}
	
	public CreateTableExecuteResult genInitResult(AllocateResult.SiteTableMap siteTable){

		CreateTableExecuteResult initResult =  new CreateTableExecuteResult();
		
		int i;
				
		for(i = 0 ; i < siteTable.tables.size() ; i++){
				String createTableSql = null;
				String fragName = siteTable.tables.get(i).toString();
				TableInfo tableinfo = gdd.getTableInfoFromFragName(fragName);
				FragmentationInfo fraginfo = gdd.getFragmentation(fragName);
				
				if(fraginfo.getFragType() == CONSTANT.FRAG_HORIZONTAL){
					Vector<ColumnInfo> columns = tableinfo.getColumnInfo();
					createTableSql = "create table "+fragName + "(";
					for(int k = 0 ; k < columns.size(); k++){
						ColumnInfo column = columns.elementAt(k);
						createTableSql += column.getColumnName()+" " + CONSTANT.DATATYPE[column.getColumnType()];
						if(column.getColumnType() == CONSTANT.VALUE_STRING)
							createTableSql += "("+column.getColumnLength()+")";
						if(column.getColumnKeyable() == 1)
							createTableSql += " key";
						if(k != columns.size() - 1)
							createTableSql += ",";
						else
							createTableSql += ");";
					}	
				}
				if(fraginfo.getFragType() == CONSTANT.FRAG_VERTICAL){
					Vector<String> columns = fraginfo.getFragConditionExpression().verticalFragmentationCondition;
					createTableSql = "create table "+fragName + "(";
					for(int k = 0 ; k < columns.size() ; k++){
						ColumnInfo columninfo = gdd.getColumnInfo(tableinfo.getTableName(), columns.elementAt(k));
						if(columninfo == null){
							System.out.println(tableinfo.getTableName()+"."+columns.elementAt(k) + " is not exist!");
							System.exit(-1);
							
						}
						createTableSql += columninfo.getColumnName()+" " + CONSTANT.DATATYPE[columninfo.getColumnType()];
						if(columninfo.getColumnType() == CONSTANT.VALUE_STRING)
							createTableSql += "("+columninfo.getColumnLength()+")";
						if(columninfo.getColumnKeyable() == 1)
							createTableSql += " key";
						if(k != columns.size() - 1)
							createTableSql += ",";
						else
							createTableSql += ");";
					}
				}
				
				//System.out.println(createTableSql);
				initResult.addSql(createTableSql);
		}
		return initResult;
	}
}
