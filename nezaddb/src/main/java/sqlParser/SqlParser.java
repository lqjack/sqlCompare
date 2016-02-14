package sqlParser;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.List;

import globalDefinition.CONSTANT;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.allocate.Allocate;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.database.CreateDatabase;
import net.sf.jsqlparser.statement.database.UseDatabase;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.fragment.HorizontalFragment;
import net.sf.jsqlparser.statement.fragment.VerticalFragment;
import net.sf.jsqlparser.statement.importdata.ImportData;
import net.sf.jsqlparser.statement.init.Init;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.setsite.SetSite;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
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
import parserResult.SelectResult;
import parserResult.SetSiteResult;
import parserResult.UseDatabaseResult;
import parserResult.VFragmentResult;
import queryTree.FormattedTreeNode;
import queryTree.QueryTree;
import queryTree.TreeOptimizer;


public class SqlParser implements StatementVisitor{
	private ParseResult parseResult;
	private String sql;
	private boolean optimized = false;
	public void setOptimizeOn(){
		optimized = true;
	}
	public void setOptimizeOff(){
		optimized = false;
	}
	public SqlParser(String sql,boolean isOptimizeOn){
		this.setSql(sql);
		this.optimized = isOptimizeOn;
		CCJSqlParserManager pm = new  CCJSqlParserManager();
		try{
			Statement st = pm.parse(new StringReader(sql));
			st.accept(this);
		}catch(JSQLParserException e){
			parseResult = new ParseErrorResult("Syntax error");
			//System.out.println();
			e.printStackTrace();
		}
	}
	@Override
	public void visit(Select select) {
		
		System.out.println(select.toString());
		QueryTree selectTree = new QueryTree();
		selectTree.setTreeType(CONSTANT.TREE_SELECT);
		selectTree.genSelectTree(select);
		selectTree.setSql(select.toString());
		selectTree.displayTree();
		parseResult = new SelectResult(selectTree);
		
		TreeOptimizer optimizer = new TreeOptimizer(selectTree);
		optimizer.setTreeOptimize(optimized);
		optimizer.queryTreeOptimize();
		System.out.println("--------optimized query tree----------");
		optimizer.getQueryTree().displayTree();
		
		//for display on web
		selectTree.genTreeList();
		List<FormattedTreeNode> nodeList = selectTree.getNodeList();
		StringBuffer results =new StringBuffer("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
		results.append("<nodes>");
		for(int i=0;i<nodeList.size();++i)
		{
			FormattedTreeNode node = (FormattedTreeNode) nodeList.get(i);
			//results += node.content+node.nodeID+node.parentNodeID+node.siteID;
			results.append("<node>");
			results.append("<content>");
			results.append(node.content);
			results.append("</content>");
			results.append("<nodeid>");
			results.append(node.nodeID);
			results.append("</nodeid>");
			results.append("<parentnodeid>");
			results.append(node.parentNodeID);
			results.append("</parentnodeid>");
			results.append("<siteid>");
			results.append(node.siteID);
			results.append("</siteid>");
			results.append("</node>");
		}
		results.append("</nodes>");
		try{
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("d:\\treeInfo.xml")),true);   
			pw.println(results.toString()); 
			pw.flush();
			pw.close();
		}catch(Exception e){}
	}

	@Override
	public void visit(Delete delete) {
		
		System.out.println("delete st");
		QueryTree deleteTree = new QueryTree();
		deleteTree.genDeleteTree(delete);
		deleteTree.setSql(delete.toString());
		parseResult = new DeleteResult(deleteTree);
		deleteTree.displayTree();
		TreeOptimizer optimizer = new TreeOptimizer(deleteTree);
		optimizer.setTreeOptimize(optimized);
		//optimizer.setTreeOptimize(true);
		optimizer.queryTreeOptimize();
		optimizer.getQueryTree().displayTree();
		
		//for display on web
		deleteTree.genTreeList();
		List<FormattedTreeNode> nodeList = deleteTree.getNodeList();
		StringBuffer results =new StringBuffer("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
		results.append("<nodes>");
		for(int i=0;i<nodeList.size();++i)
		{
			FormattedTreeNode node = (FormattedTreeNode) nodeList.get(i);
			//results += node.content+node.nodeID+node.parentNodeID+node.siteID;
			results.append("<node>");
			results.append("<content>");
			if(node.content.startsWith("Selection")){
				String content = node.content.substring(9);
				content ="Delete"+content;
				results.append(content);
			}
			else
				results.append(node.content);
			results.append("</content>");
			results.append("<nodeid>");
			results.append(node.nodeID);
			results.append("</nodeid>");
			results.append("<parentnodeid>");
			results.append(node.parentNodeID);
			results.append("</parentnodeid>");
			results.append("<siteid>");
			results.append(node.siteID);
			results.append("</siteid>");
			results.append("</node>");
		}
		results.append("</nodes>");
		try{
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("d:\\treeInfo.xml")),true);   
			pw.println(results.toString()); 
			pw.flush();
			pw.close();
		}catch(Exception e){}
	}

	@Override
	public void visit(Update update) {
		
		System.out.println("command 'UPDATE' is not supported!");
		
	}

	@Override
	public void visit(Insert insert) {
		
		System.out.println("insert st");
		QueryTree insertTree = new QueryTree();
		insertTree.genInsertTree(insert);
		insertTree.setSql(insert.toString());
		parseResult = new InsertResult(insertTree);
		insertTree.displayTree();
		TreeOptimizer optimizer = new TreeOptimizer(insertTree);
		optimizer.setTreeOptimize(optimized);
		optimizer.queryTreeOptimize();
		optimizer.getQueryTree().displayTree();
		
		
		
		
		//for queryTree display on web
		insertTree.genTreeList();
		List<FormattedTreeNode> nodeList = insertTree.getNodeList();
		StringBuffer results =new StringBuffer("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
		results.append("<nodes>");
		for(int i=0;i<nodeList.size();++i)
		{
			FormattedTreeNode node = (FormattedTreeNode) nodeList.get(i);
			//results += node.content+node.nodeID+node.parentNodeID+node.siteID;
			results.append("<node>");
			results.append("<content>");
			if(node.content.startsWith("Selection")){
				String content = node.content.substring(9);
				content ="Insert"+content;
				results.append(content);
			}
			else
				results.append(node.content);
			results.append("</content>");
			results.append("<nodeid>");
			results.append(node.nodeID);
			results.append("</nodeid>");
			results.append("<parentnodeid>");
			results.append(node.parentNodeID);
			results.append("</parentnodeid>");
			results.append("<siteid>");
			results.append(node.siteID);
			results.append("</siteid>");
			results.append("</node>");
		}
		results.append("</nodes>");
		try{
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("d:\\treeInfo.xml")),true);   
			pw.println(results.toString()); 
			pw.flush();
			pw.close();
		}catch(Exception e){}
	}

	@Override
	public void visit(Replace replace) {
		
		System.out.println("command 'RELPACE' is not supported!");
	}

	@Override
	public void visit(Drop drop) {
		
		System.out.println("command 'DROP' is not supported!");
	}

	@Override
	public void visit(Truncate truncate) {
		
		System.out.println("command 'TRUNCATE' is not supported!");
	}

	@Override
	public void visit(CreateTable createTable) {
		
		System.out.println("create table parse successfully");
		parseResult = new CreateTableResult(createTable.getTable().getName(),createTable.getColumnDefinitions(),createTable.getTableOptionsStrings(),createTable.getIndexes());
	}

	@Override
	public void visit(VerticalFragment fragment) {
		
		System.out.println("vertical fragment parse successfully!");
		parseResult = new VFragmentResult(fragment.getTable().getName(), fragment.getSubTableList(),fragment.getColumns());
	}

	@Override
	public void visit(HorizontalFragment fragment) {
		
		System.out.println("horizontal fragment parse successfully!");
		parseResult =  new HFragmentResult(fragment.getTable().getName(),fragment.getSubTableList(),fragment.getConditions());
	}

	@Override
	public void visit(Allocate allocate) {
		
		System.out.println("allocate parse successfully!");
		parseResult = new AllocateResult(allocate.getSiteList(),allocate.getTableList());
	}

	@Override
	public void visit(ImportData importdata) {
		
		System.out.println("importdata parse successfully!");
		parseResult = new ImportDataResult(importdata.getFileList());
		
	}

	@Override
	public void visit(SetSite setSite) {
		
		System.out.println("setsite parse successfully!");
		parseResult = new SetSiteResult(setSite.getSiteNameList(),setSite.getIpList(),setSite.getPortList());
	}
	
	@Override
	public void visit(CreateDatabase createDatabase) {
		
		System.out.println("createdb parse successfully!");
		parseResult = new CreateDatabaseResult(createDatabase.getDatabaseName());
		
	}
	
	@Override
	public void visit(UseDatabase useDatabase) {
		
		System.out.println("usedb parse successfully!");
		parseResult = new UseDatabaseResult(useDatabase.getDatabaseName());
		
	}
	
	@Override
	public void visit(Init init) {
		
		System.out.println("init parse successfully!");
		parseResult = new InitResult(init.getFileName());
	}
	public void setResult(ParseResult result) {
		this.parseResult = result;
	}
	public ParseResult getResult() {
		return parseResult;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public String getSql() {
		return sql;
	}

}
