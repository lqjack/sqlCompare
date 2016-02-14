package execute;

import java.util.*;
import java.io.*;

import executeResult.ExecuteResult;
import executeResult.InitExecuteResult;
import executeResult.InitExecuteResultUnit;

import sqlParser.SqlParser;
import parserResult.*;
import parserResult.HFragmentResult.HFragmentUnit;
import gdd.*;
import globalDefinition.*;


public class InitExecute extends ExecuteSQL implements ParseResultVisitor{
	private GDD gdd;
	
	InitExecute(){
		gdd = GDD.getInstance();
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		InitResult initResult = (InitResult)result;
		String filePath = initResult.getFileName();
		initGDD(filePath);
	}
	
	public void allocateTaskToSites(InitExecuteResult initResult){
		
	}
	
	public InitExecuteResult genInitResult(){
		InitExecuteResult initResult = null;
		
		initResult =  new InitExecuteResult();
		
		int i,j;
		Vector<SiteInfo>siteinfos = gdd.getSiteInfo();
		for(i = 0 ; i < siteinfos.size() ; i++){
			InitExecuteResultUnit initUnit = new InitExecuteResultUnit();
			initUnit.createDBSql = "create database " + gdd.getDBName() + ";";
			initUnit.usageDBSql = "use "+gdd.getDBName()+";";
			SiteInfo siteinfo = siteinfos.elementAt(i);
			for(j = 0 ; j < siteinfo.getSiteFragNames().size(); j++){
				String createTableSql = null;
				String fragName = siteinfo.getSiteFragNames().elementAt(j);
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
				initUnit.createTableSql.add(createTableSql);
			}
			initResult.getInitResultUnits().add(initUnit);
		}
		
		
		return initResult;
	}
	
	
	void initGDD(String filePath){
		BufferedReader br = null;
		
		try{
			filePath = "upload/"+filePath;
			File file = new File(filePath);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		}catch(FileNotFoundException e){
			System.out.println("file "+ filePath + "not found");
			System.exit(-1);
		}catch(IOException e){
			System.out.println("io operate error");
			e.printStackTrace();
			System.exit(-1);
		}
		
		try{
			String sql;
			sql = br.readLine();
			while(sql != null && sql.length() > 0){
				SqlParser v= new SqlParser(sql,false);
				v.getResult().accept(this);
				sql = br.readLine();
			}
		}catch(IOException e){
			System.out.println("io exception error in read file "+ filePath);
			e.printStackTrace();
			System.exit(-1);
		}
		
		
	}
	
	@Override
	public void visit(AllocateResult allocateResult) {
		// TODO Auto-generated method stub
		AllocateExecute allocate = new AllocateExecute();
		allocate.execute(allocateResult);
		
	}

	@Override
	public void visit(HFragmentResult fragmentResult) {
		// TODO Auto-generated method stub
		HFragmentExecute hfragment = new HFragmentExecute();
		hfragment.execute(fragmentResult);
	}

	@Override
	public void visit(ParseErrorResult parserErrorResult) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectResult selectResult) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SetSiteResult setSiteResult) {
		// TODO Auto-generated method stub
		
		SetSiteExecute setSite = new SetSiteExecute();
		setSite.execute(setSiteResult);
		
	}

	@Override
	public void visit(VFragmentResult fragmentResult) {
		// TODO Auto-generated method stub
		//fragmentResult.displayResult();
		VFragmentExecute vfragment = new VFragmentExecute();
		vfragment.execute(fragmentResult);
		
	}

	@Override
	public void visit(ImportDataResult importDataResult) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateTableResult createTableResult) {
		// TODO Auto-generated method stub
		CreateTableExecute createTable = new CreateTableExecute();
		createTable.execute(createTableResult);	
	}

	@Override
	public void visit(InsertResult insertResult) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DeleteResult deleteResult) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateDatabaseResult createDatabaseResult) {
		// TODO Auto-generated method stub
		//gdd.setDBName(createDatabaseResult.getDatabaseName());
		CreateDBExecute createDb = new CreateDBExecute();
		createDb.execute(createDatabaseResult);
	}

	@Override
	public void visit(UseDatabaseResult useDatabaseResult) {
		// TODO Auto-generated method stub
		UseDBExecute useDb = new UseDBExecute();
		useDb.execute(useDatabaseResult);
		
	}

	@Override
	public void visit(InitResult initResult) {
		// TODO Auto-generated method stub
		
	}



	
	
}