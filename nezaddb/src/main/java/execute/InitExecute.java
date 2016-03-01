package execute;

import executeResult.InitExecuteResult;
import executeResult.InitExecuteResultUnit;
import gdd.ColumnInfo;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteMeta;
import gdd.TableMeta;
import globalDefinition.CONSTANT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
import sqlParser.SqlParser;

public class InitExecute extends ExecuteSQL implements ParseResultVisitor {
	private GDD gdd;

	InitExecute() {
		gdd = GDD.getInstance();
	}

	@Override
	public void execute(ParseResult result) {
		InitResult initResult = (InitResult) result;
		String filePath = initResult.getFileName();
		initGDD(filePath);
	}

	public void allocateTaskToSites(InitExecuteResult initResult) {
	}

	public InitExecuteResult genInitResult() {
		InitExecuteResult initResult = null;
		initResult = new InitExecuteResult();
		int i, j;
		Vector<SiteMeta> siteinfos = gdd.getSiteInfo();
		for (i = 0; i < siteinfos.size(); i++) {
			InitExecuteResultUnit initUnit = new InitExecuteResultUnit();
			initUnit.createDBSql = "create database " + gdd.getDBName() + ";";
			initUnit.usageDBSql = "use " + gdd.getDBName() + ";";
			SiteMeta siteinfo = siteinfos.elementAt(i);
			for (j = 0; j < siteinfo.getSiteFragNames().size(); j++) {
				String createTableSql = null;
				String fragName = siteinfo.getSiteFragNames().elementAt(j);
				TableMeta tableinfo = gdd.getTableInfoFromFragName(fragName);
				FragmentationInfo fraginfo = gdd.getFragmentation(fragName);

				if (fraginfo.getFragType() == CONSTANT.FRAG_HORIZONTAL) {
					Vector<ColumnInfo> columns = tableinfo.getColumnInfo();
					createTableSql = "create table " + fragName + "(";
					for (int k = 0; k < columns.size(); k++) {
						ColumnInfo column = columns.elementAt(k);
						createTableSql += column.getColumnName() + " "
								+ CONSTANT.DATATYPE[column.getColumnType()];
						if (column.getColumnType() == CONSTANT.VALUE_STRING)
							createTableSql += "(" + column.getColumnLength()
									+ ")";
						if (column.getColumnKeyable() == 1)
							createTableSql += " key";
						if (k != columns.size() - 1)
							createTableSql += ",";
						else
							createTableSql += ");";
					}
				}
				if (fraginfo.getFragType() == CONSTANT.FRAG_VERTICAL) {
					Vector<String> columns = fraginfo
							.getFragConditionExpression().verticalFragmentationCondition;
					createTableSql = "create table " + fragName + "(";
					for (int k = 0; k < columns.size(); k++) {
						ColumnInfo columninfo = gdd.getColumnInfo(
								tableinfo.getTableName(), columns.elementAt(k));
						if (columninfo == null) {
							System.out.println(tableinfo.getTableName() + "."
									+ columns.elementAt(k) + " is not exist!");
							System.exit(-1);

						}
						createTableSql += columninfo.getColumnName() + " "
								+ CONSTANT.DATATYPE[columninfo.getColumnType()];
						if (columninfo.getColumnType() == CONSTANT.VALUE_STRING)
							createTableSql += "("
									+ columninfo.getColumnLength() + ")";
						if (columninfo.getColumnKeyable() == 1)
							createTableSql += " key";
						if (k != columns.size() - 1)
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

	void initGDD(String filePath) {
		BufferedReader br = null;
		try {
			filePath = "upload/" + filePath;
			File file = new File(filePath);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file)));
		} catch (FileNotFoundException e) {
			System.out.println("file " + filePath + "not found");
			System.exit(-1);
		}
		try {
			String sql;
			sql = br.readLine();
			while (sql != null && sql.length() > 0) {
				SqlParser v = new SqlParser(sql, false);
				v.getResult().accept(this);
				sql = br.readLine();
			}
		} catch (IOException e) {
			System.out.println("io exception error in read file " + filePath);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@Override
	public void visit(AllocateResult allocateResult) {
		AllocateExecute allocate = new AllocateExecute();
		allocate.execute(allocateResult);
	}

	@Override
	public void visit(HFragmentResult fragmentResult) {
		HFragmentExecute hfragment = new HFragmentExecute();
		hfragment.execute(fragmentResult);
	}

	@Override
	public void visit(ParseErrorResult parserErrorResult) {
	}

	@Override
	public void visit(SelectResult selectResult) {
	}

	@Override
	public void visit(SetSiteResult setSiteResult) {
		SetSiteExecute setSite = new SetSiteExecute();
		setSite.execute(setSiteResult);
	}

	@Override
	public void visit(VFragmentResult fragmentResult) {
		// fragmentResult.displayResult();
		VFragmentExecute vfragment = new VFragmentExecute();
		vfragment.execute(fragmentResult);
	}

	@Override
	public void visit(ImportDataResult importDataResult) {
	}

	@Override
	public void visit(CreateTableResult createTableResult) {
		CreateTableExecute createTable = new CreateTableExecute();
		createTable.execute(createTableResult);
	}

	@Override
	public void visit(InsertResult insertResult) {
	}

	@Override
	public void visit(DeleteResult deleteResult) {
	}

	@Override
	public void visit(CreateDatabaseResult createDatabaseResult) {
		// gdd.setDBName(createDatabaseResult.getDatabaseName());
		CreateDBExecute createDb = new CreateDBExecute();
		createDb.execute(createDatabaseResult);
	}

	@Override
	public void visit(UseDatabaseResult useDatabaseResult) {
		UseDBExecute useDb = new UseDBExecute();
		useDb.execute(useDatabaseResult);
	}

	@Override
	public void visit(InitResult initResult) {
	}
}