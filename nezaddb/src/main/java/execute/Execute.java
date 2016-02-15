package execute;

import executeReturnResult.ErrorReturnResult;
import executeReturnResult.ExecuteReturnResult;
import executeReturnResult.ParseTreeReturnResult;
import gdd.GDD;
import gdd.SiteInfo;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Vector;

import parserResult.AllocateResult;
import parserResult.CreateDatabaseResult;
import parserResult.CreateTableResult;
import parserResult.DeleteResult;
import parserResult.HFragmentResult;
import parserResult.ImportDataResult;
import parserResult.InitResult;
import parserResult.InsertResult;
import parserResult.ParseErrorResult;
import parserResult.ParseResult;
import parserResult.ParseResultVisitor;
import parserResult.SelectResult;
import parserResult.SetSiteResult;
import parserResult.UseDatabaseResult;
import parserResult.VFragmentResult;
import queryTree.QueryTree;
import sqlParser.SqlParser;

import communication.ClientBase;

public class Execute implements ParseResultVisitor{
	private GDD gdd;
	
	private ExecuteReturnResult returnResult;
	
	public Execute(){
		gdd = GDD.getInstance();
		gdd.GDDReader(GDD.CONTROL_SERVER_CONFIG);
	}
	
	public Execute(String driverClass,String url,String user,String password) throws SQLException, ReflectiveOperationException{
//		gdd = GDD.getInstance(driverClass, url, user, password);
	}
	
	public void execute(String sql, boolean opt){
	    try {
	        SqlParser parser = new SqlParser(sql,opt);
	        parser.getResult().accept(this);
	        if (parser.getResult() instanceof SelectResult)
	            return;
	    } catch (Exception ex) {
	        returnResult = new ErrorReturnResult("Parser : failed , you may check whether the table exist");
	    }
	    
//TODO : should not send the data to site
/*		try{
			if(sendGDDFileToSites()){
				System.out.println("transfer gdd file to all sites succeed!");
			}
		}catch(IOException e){
			System.out.println(e.toString());
		}
*/	}
	
	public ExecuteReturnResult getParseTree(String sql, boolean opt) {
	    ExecuteReturnResult result = null;
	    SqlParser parser = null;
	    ParseResult pr = null;
	    try {
	        parser = new SqlParser(sql,opt);
	        pr = parser.getResult();
	    } catch (Exception ex) {
	        result = new ErrorReturnResult("Parser : failed , you may check whether the table exist");
	        return result;
	    }
        QueryTree tree = null;
        if (pr instanceof SelectResult) {
            SelectResult select = (SelectResult)pr;
            tree = select.getSelectTree();
        }
        
        if (pr instanceof ParseErrorResult) {
            ParseErrorResult error = (ParseErrorResult)pr;
            result = new ErrorReturnResult("Parser: " + error.getErrorInfo());
        }
        
        if (tree != null) {
            result = new ParseTreeReturnResult(tree.getNodeList());
        }
        
        return result;
	}
	
	public void parse(String sql) {
	}
	
	public ExecuteReturnResult getReturnResult(){
		return this.returnResult;
	}
	
	
	public boolean sendGDDFileToSites()throws IOException{ 
		Vector<SiteInfo> siteInfos = gdd.getSiteInfo();
		for(int i = 0 ; i < siteInfos.size() ; i++){
			SiteInfo siteinfo = siteInfos.elementAt(i);
			ClientBase client = new ClientBase(siteinfo.getSiteIP(),siteinfo.getSitePort());
			System.out.println("gddfile transfer client create success!");
			File transFile = new File(GDD.CONTROL_SERVER_CONFIG);
			String rs = client.sendFile("gddfile", transFile, "gdd.config", "text/plain");
			System.out.println("transfer gdd file "+ rs );
		}
		return true;
	}
	
	
	
	@Override
	public void visit(AllocateResult allocateResult) {
		AllocateExecute allocate = new AllocateExecute();
		allocate.execute(allocateResult);
		gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
		this.returnResult = allocate.getResult();
	}

	@Override
	public void visit(HFragmentResult fragmentResult) {
		HFragmentExecute hfragment = new HFragmentExecute();
		hfragment.execute(fragmentResult);
		gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
	}

	@Override
	public void visit(ParseErrorResult parserErrorResult) {
	    returnResult = new ErrorReturnResult("Parser: " + parserErrorResult.getErrorInfo());
	}

	@Override
	public void visit(SelectResult selectResult) {
		System.out.println("#############Execute : Select");
		SelectExecute selectExecute = new SelectExecute();
		selectExecute.execute(selectResult);
		returnResult = selectExecute.getResult();
		System.out.println("#############Execute : Select End");
	}

	@Override
	public void visit(SetSiteResult setSiteResult) {
		SetSiteExecute setSite = new SetSiteExecute();
		setSite.execute(setSiteResult);
		gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);
	}
	
	
	@Override
	public void visit(VFragmentResult fragmentResult) {
		VFragmentExecute vfragment = new VFragmentExecute();
		vfragment.execute(fragmentResult);
		gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
	}

	@Override
	public void visit(ImportDataResult importDataResult) {
		ImportExecute importExecute = new ImportExecute();
		importExecute.execute(importDataResult);
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
		this.returnResult = importExecute.getResult();
		
	}

	@Override
	public void visit(CreateTableResult createTableResult) {
		CreateTableExecute createTable = new CreateTableExecute();
		createTable.execute(createTableResult);
		gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
	}

	@Override
	public void visit(InsertResult insertResult) {
		//insertResult.displayResult();
		InsertExecute insert = new InsertExecute();
		insert.execute(insertResult);
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
		this.returnResult = insert.getResult();
	}

	@Override
	public void visit(DeleteResult deleteResult) {
		//deleteResult.displayResult();
		DeleteExecute delete = new DeleteExecute();
		delete.execute(deleteResult);
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);		
	}

	@Override
	public void visit(CreateDatabaseResult createDatabaseResult){
		CreateDBExecute createDb = new CreateDBExecute();
		createDb.execute(createDatabaseResult);	
		//gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
	}

	@Override
	public void visit(UseDatabaseResult useDatabaseResult) {
		
		UseDBExecute useDb = new UseDBExecute();
		useDb.execute(useDatabaseResult);
		gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
	}

	@Override
	public void visit(InitResult initResult) {
		
		InitExecute init = new InitExecute();
		init.execute(initResult);
		//gdd.printGDD();
		gdd.GDDWriter(GDD.CONTROL_SERVER_CONFIG);	
	}
}