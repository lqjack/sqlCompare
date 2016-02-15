package execute;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import communication.ClientBase;

import executeResult.CreateDBExecuteResult;
import executeResult.ImportExecuteResult;
import executeResult.ImportExecuteResultUnit;
import executeReturnResult.ExecuteReturnResult;
import executeReturnResult.ImportReturnResult;

import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteMeta;
import gdd.TableMeta;
import gdd.Utility;
import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;
import parserResult.ImportDataResult;
import parserResult.ParseResult;
import slaveExecuteResult.SlaveImportResult;

public class ImportExecute extends ExecuteSQL{
	
	private GDD gdd;
	private ImportExecuteResult importResultSite1;
	private ImportExecuteResult importResultSite2;
	private ImportExecuteResult importResultSite3;
	private ImportExecuteResult importResultSite4;
	
	
	private ImportReturnResult importReturnResult;
	
	public ImportExecute(){
		gdd = GDD.getInstance();
	}
	
	
	public ExecuteReturnResult getResult(){
		return this.importReturnResult;
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		ImportDataResult importResult = (ImportDataResult)result;
		List fileList = importResult.getFileList();
		
		importResult.displayResult();
		
		long time1,time2,time3;
		time1 = System.currentTimeMillis();
		
		if (genImportResult(fileList)){   //get import data
			//this.displayResult();
			time2 = System.currentTimeMillis();
			System.out.println("get result time:"+(time2-time1));
			
			
			
			try{
				allocateTaskSites(); //allocate the task to all sites
			}catch(IOException ex){
				System.out.println(ex.toString());
				ex.printStackTrace();
			}
			
			time3 = System.currentTimeMillis();
			System.out.println("alloc task time:"+(time3-time2));
			
			if(!updateGDDInfo()){
				System.out.println("Error: update gdd info error!");
			}
		}
	}
	
	public boolean updateGDDInfo(){
		
		boolean flag = true;
		
		if(this.importResultSite1.getImportUnits() != null)
			flag &= updateGDDInfoFromImportResult(importResultSite1);
		if(this.importResultSite2.getImportUnits() != null)
			flag &= updateGDDInfoFromImportResult(importResultSite2);
		if(this.importResultSite3.getImportUnits() != null)
			flag &= updateGDDInfoFromImportResult(importResultSite3);
		if(this.importResultSite4.getImportUnits() != null)
			flag &= updateGDDInfoFromImportResult(importResultSite4);
	
		return flag;
	}
	
	public boolean updateGDDInfoFromImportResult(ImportExecuteResult importResult){
		Vector<ImportExecuteResultUnit> importUnits = importResult.getImportUnits();
		for(int i = 0 ; i < importUnits.size() ; i++){
			ImportExecuteResultUnit unit = importUnits.elementAt(i);
			FragmentationInfo fragInfo = gdd.getFragmentation(unit.tableName);
			if(fragInfo == null)
				return false;
			if(unit.columnInfos != null)
				fragInfo.setFragSize(unit.columnInfos.size());
		}
		return true;
	}
	
	
	public boolean allocateTaskSites() throws IOException{
		boolean flag = true;
		
		Vector<SiteMeta> siteinfos  = gdd.getSiteInfo();
		SiteMeta siteinfo = null;
		ClientBase client;
		boolean r;
		
		this.importReturnResult = new ImportReturnResult();
		
		if(this.importResultSite1.getImportUnits() != null){
			siteinfo = siteinfos.elementAt(0);
			client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
			System.out.println("client create success");
			Object result = client.sendContext("importtable", this.importResultSite1);	
			System.out.println("site1 over!");
			
			if(!(result instanceof SlaveImportResult)){
				System.out.println("error object");
			}
			else{
				SlaveImportResult slaveImportResult = (SlaveImportResult)result;
				String s;
				if(slaveImportResult.getSuccess()){
					s = "import data into "+siteinfo.getSiteName()+" succeed!";
				}
				else{
					s = "import data into "+siteinfo.getSiteName()+" failed!" + slaveImportResult.getErrorInfo();
				}
				this.importReturnResult.addInfo(s);
			}
		}
		
		if(this.importResultSite2.getImportUnits() != null){
			siteinfo = siteinfos.elementAt(1);
			client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
			System.out.println("client create success");
			Object result = client.sendContext("importtable", this.importResultSite2);	
			System.out.println("site2 over!");
			
			if(!(result instanceof SlaveImportResult)){
				System.out.println("error object");
			}
			else{
				SlaveImportResult slaveImportResult = (SlaveImportResult)result;
				String s;
				if(slaveImportResult.getSuccess()){
					s = "import data into "+siteinfo.getSiteName()+" succeed!";
				}
				else{
					s = "import data into "+siteinfo.getSiteName()+" failed!" + slaveImportResult.getErrorInfo();
				}
				this.importReturnResult.addInfo(s);
			}
		}
	    
		if(this.importResultSite3.getImportUnits() != null){
			siteinfo = siteinfos.elementAt(2);
			client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
			System.out.println("client create success");
			Object result = client.sendContext("importtable", this.importResultSite3);	
			System.out.println("site3 over!");
			
			if(!(result instanceof SlaveImportResult)){
				System.out.println("error object");
			}
			else{
				SlaveImportResult slaveImportResult = (SlaveImportResult)result;
				String s;
				if(slaveImportResult.getSuccess()){
					s = "import data into "+siteinfo.getSiteName()+" succeed!";
				}
				else{
					s = "import data into "+siteinfo.getSiteName()+" failed!" + slaveImportResult.getErrorInfo();
				}
				this.importReturnResult.addInfo(s);
			}
		}
	    
		if(this.importResultSite4.getImportUnits() != null){
			siteinfo = siteinfos.elementAt(3);
			client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
			System.out.println("client create success");
			Object result = client.sendContext("importtable", this.importResultSite4);	
			System.out.println("site4 over!");
			
			if(!(result instanceof SlaveImportResult)){
				System.out.println("error object");
			}
			else{
				SlaveImportResult slaveImportResult = (SlaveImportResult)result;
				String s;
				if(slaveImportResult.getSuccess()){
					s = "import data into "+siteinfo.getSiteName()+" succeed!";
				}
				else{
					s = "import data into "+siteinfo.getSiteName()+" failed!" + slaveImportResult.getErrorInfo();
				}
				this.importReturnResult.addInfo(s);
			}
		}
	    
		return true;		
	}
	
	public boolean genImportResult(List fileList){
		
		this.importResultSite1 = new ImportExecuteResult();
		this.importResultSite2 = new ImportExecuteResult();
		this.importResultSite3 = new ImportExecuteResult();
		this.importResultSite4 = new ImportExecuteResult();
		
		int i;
		String filepath;
		BufferedReader br = null;
		int fileNum = fileList.size();
		
		//System.out.println("fileNum="+fileList.size());
		
		
		for(i = 0 ; i < fileList.size() ; i++){
			filepath = fileList.get(i).toString();
			//System.out.println("filepath="+filepath);
			
			try{
				Vector<String> strs = Utility.StringTokener(filepath,"/");
				filepath = strs.elementAt(strs.size() -1);
				filepath = "upload/"+filepath;
				File file = new File(filepath);
				br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			}catch(FileNotFoundException e){
				System.out.println(e.toString());
				return false;	
			}catch(IOException e){
				System.out.println(e.toString());
				return false;	
			}
			
			if(!genImportResultFromFile(br,filepath)){
				System.out.println("Error:error in import file " + filepath);
				return false;
			}
		}
		
		return true;
	}
	
	public boolean genImportResultFromFile(BufferedReader br,String filepath){
		String str;
		String tableName;
		Vector<String> columns;
		int colNum,index1,index2;
		String columnString,colNumString;
		int i,index;
		int fragType;
		
		Vector<String> strs = Utility.StringTokener(filepath,"/");
		str = strs.elementAt(strs.size() -1);
		index = str.indexOf(".");
		if(index == -1)
			tableName = str;
		else
			tableName = str.substring(0, index);
		System.out.println("tableName="+tableName);
		
		TableMeta tableinfo = gdd.getTableInfo(tableName);
		columns = tableinfo.getColumnNames();
		fragType = tableinfo.getFragType();
		colNum = 0;
		switch(fragType){
			case CONSTANT.FRAG_HORIZONTAL:
				return genImportUnitsHorizontal(br,tableName,columns,colNum);
			case CONSTANT.FRAG_VERTICAL:
				return genImportUnitsVertical(br,tableName,columns,colNum);
			case CONSTANT.FRAG_HYBIRD:
				return genImportUnitsHybird(br,tableName,columns,colNum);
		}
		return true;
	}
	


	public boolean genImportUnits(BufferedReader br,String tableName,Vector<String>columns,int colNum){
		int i;
		String str;
		int fragType;
		TableMeta tableinfo = gdd.getTableInfo(tableName);
		fragType = tableinfo.getFragType();
		switch(fragType){
			case CONSTANT.FRAG_HORIZONTAL:
				return genImportUnitsHorizontal(br,tableName,columns,colNum);
			case CONSTANT.FRAG_VERTICAL:
				return genImportUnitsVertical(br,tableName,columns,colNum);
			case CONSTANT.FRAG_HYBIRD:
				return genImportUnitsHybird(br,tableName,columns,colNum);
		}
		return true;
	}
	
	public boolean genImportUnitsHorizontal(BufferedReader br,String tableName,Vector<String>columns,int colNum){
		String str;
		TableMeta tableinfo = gdd.getTableInfo(tableName);
		int fragSize = tableinfo.getFragNum(); 
		Vector<FragmentationInfo>fragInfos = tableinfo.getFragmentationInfo();
		
		Vector<ImportExecuteResultUnit> importUnits = new Vector();
		Vector<Vector<SimpleExpression>> expressions = new Vector();
		if(fragSize != fragInfos.size()){
			System.out.println("error: the number of fragSize isn't right!");
			return false;
		}
		
		for(int i = 0 ; i < fragSize ; i++){
			FragmentationInfo fraginfo = fragInfos.elementAt(i);
			ImportExecuteResultUnit unit = new ImportExecuteResultUnit();
			unit.siteName = fraginfo.getFragSiteName();
			unit.tableName = fraginfo.getFragName();
			unit.columnNames = columns;
			unit.columnNameString = Utility.stringFromTokener(unit.columnNames);
			unit.columnInfos = new Vector();
			Vector<Integer> index  = new Vector();
			importUnits.add(unit);
			expressions.add(fraginfo.getFragConditionExpression().HorizontalFragmentationCondition);
		}
		
		try{
				str  = br.readLine();
				while(str != null){
					for(int j = 0 ; j < expressions.size(); j++){
						if(judgeRecord(expressions.elementAt(j),str,columns)){
							str = str.replace('\t', ',');
							str = "(" + str + ")";
							importUnits.elementAt(j).columnInfos.add(str);
							break;
						}		
					}
					str = br.readLine();
				}
				
		}catch(IOException e){
			System.out.println(e.toString());
			return false;	
		}
		
		
		
		String sitename;
		for(int i = 0 ; i < importUnits.size() ; i++){
			sitename = importUnits.elementAt(i).siteName;
			if(sitename.equals("site1")){
				this.importResultSite1.addImportExecuteResult(importUnits.elementAt(i));
			}else if(sitename.equals("site2")){
				this.importResultSite2.addImportExecuteResult(importUnits.elementAt(i));
			}else if(sitename.equals("site3")){
				this.importResultSite3.addImportExecuteResult(importUnits.elementAt(i));
			}else if(sitename.equals("site4")){
				this.importResultSite4.addImportExecuteResult(importUnits.elementAt(i));
			}else{
				System.out.println("Error: error sitename!");
				return false;
			}
		}
		
		
		return true;
	}
	
	boolean judgeRecord(Vector<SimpleExpression>expressions,String str,Vector<String>colNames){
		Map map = Utility.StringToMap(colNames, str);
		
		if(map == null){
			System.out.println("Error:str format error!");
			return false;
		}
		
		String value;
		for(int i = 0 ; i < expressions.size() ; i++){
			SimpleExpression expression = expressions.elementAt(i);
			value = expression.value;
			if(!(map.containsKey(expression.columnName) && judgeValue(expression.op,map.get(expression.columnName).toString(),value))){
				return false;
			}
		}
		
		return true;	
	}
	
	public boolean judgeValue(String op,String value1,String value2){
		int v1,v2;
		
		if(op.equals("=")){
			return value1.equals(value2);
		}else if(op.equals(">")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 > v2);
		}else if(op.equals("<")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 < v2);
		}else if(op.equals(">=")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 >= v2);
		}else if(op.equals("<=")){
			v1 = Integer.parseInt(value1);
			v2 = Integer.parseInt(value2);
			return (v1 <= v2);
		}else if(op.equals("==")){
			return !value1.equals(value2);
		}else{
			System.out.println("Error: error op!");
			return false;
		}
	}
	
	public boolean genImportUnitsVertical(BufferedReader br,String tableName,Vector<String>columns,int colNum){
		String str;
		TableMeta tableinfo = gdd.getTableInfo(tableName);
		int fragSize = tableinfo.getFragNum();
		Vector<FragmentationInfo>fragInfos = tableinfo.getFragmentationInfo();
		Vector<ImportExecuteResultUnit> importUnits = new Vector();
		Vector<Vector<Integer>> indexes = new Vector();
		if(fragSize != fragInfos.size()){
			System.out.println("error: the number of fragSize isn't right!");
			return false;
		}
		
		for(int i = 0 ; i < fragSize ; i++){
			FragmentationInfo fraginfo = fragInfos.elementAt(i);
			ImportExecuteResultUnit unit = new ImportExecuteResultUnit();
			unit.siteName = fraginfo.getFragSiteName();
			unit.tableName = fraginfo.getFragName();
			unit.columnNames = fraginfo.getFragConditionExpression().verticalFragmentationCondition;
			unit.columnNameString = Utility.stringFromTokener(unit.columnNames);
			unit.columnInfos = new Vector();
			Vector<Integer> index  = new Vector();
			for(int j = 0 ; j < unit.columnNames.size() ; j++)
				for(int k= 0 ; k < columns.size() ; k++)
					if(unit.columnNames.elementAt(j).equals(columns.elementAt(k)))
						index.add(k);
			indexes.add(index);
			importUnits.add(unit);
		}
		
		
		Vector<String> result;
		try{
				while((str = br.readLine()) != null){
					result = genStringsFromStr(str,indexes);
					for(int j = 0 ; j < result.size() ; j++){
						importUnits.elementAt(j).columnInfos.add(result.elementAt(j));
					}
				}
		}catch(IOException e){
			System.out.println(e.toString());
			return false;	
		}
		
		String sitename;
		for(int i = 0 ; i < importUnits.size() ; i++){
			sitename = importUnits.elementAt(i).siteName;
			if(sitename.equals("site1")){
				this.importResultSite1.addImportExecuteResult(importUnits.elementAt(i));
			}else if(sitename.equals("site2")){
				this.importResultSite2.addImportExecuteResult(importUnits.elementAt(i));
			}else if(sitename.equals("site3")){
				this.importResultSite3.addImportExecuteResult(importUnits.elementAt(i));
			}else if(sitename.equals("site4")){
				this.importResultSite4.addImportExecuteResult(importUnits.elementAt(i));
			}else{
				System.out.println("Error: error sitename!");
				return false;
			}
		}
		
		
		return true;
	}
	
	public boolean genImportUnitsHybird(BufferedReader br,String tableName,Vector<String>columns,int colNum){
		return true;
	}
	
	public Vector<String> genStringsFromStr(String str,Vector<Vector<Integer>> indexes){
		Vector<String> result = new Vector();
		Vector<String> strs = Utility.StringTokener(str,"	");
		Vector<Integer> index;
		for(int i = 0 ; i < indexes.size() ; i++){
			String s = "(";
			index = indexes.elementAt(i);
			for(int j = 0 ; j < (index.size()-1) ; j++){
				s += strs.elementAt(index.elementAt(j))+",";
			}
			s += strs.elementAt(index.elementAt(index.size() -1))+")";
			result.add(s);
		}
		return result;
	}
	
	public void displayResult(){
		System.out.println("importResultSite1:");
		this.importResultSite1.displayResult();
		
		System.out.println("importResultSite2:");
		this.importResultSite2.displayResult();
		
		System.out.println("importResultSite3:");
		this.importResultSite3.displayResult();
		
		System.out.println("importResultSite4:");
		this.importResultSite4.displayResult();
		
	}

}
