package gdd;

import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.culturegraph.mf.sql.connection.ConnectionUtils;

import com.github.jinahya.sql.database.metadata.bind.Catalog;
import com.github.jinahya.sql.database.metadata.bind.Column;
import com.github.jinahya.sql.database.metadata.bind.MetadataContext;
import com.github.jinahya.sql.database.metadata.bind.Schema;
import com.github.jinahya.sql.database.metadata.bind.Table;

class AllSite {
	Map<String, SiteMeta> sites;

	void add(SiteMeta site) {
		sites.put(site.name, site);
	}

	String getFullName(ColumnMeta column) throws ColumnNotFoundException,
			MutipleDefitionExeception {
		int foundNum = 0;
		String fullColumnName = "";
		for (Entry<String, SiteMeta> entrySite : sites.entrySet()) {
			fullColumnName = entrySite.getValue().getColumnPartName(column);
			if (fullColumnName != null && !fullColumnName.equals(""))
				foundNum++;
		}
		if (foundNum < 1)
			throw new ColumnNotFoundException(column.name + " not found ");
		if (foundNum > 1)
			throw new MutipleDefitionExeception(column.name
					+ " have mutiple definition ");
		return fullColumnName;
	}
}

class SiteMeta extends Description {
	List<CatalogMeta> catalogs;

	void add(CatalogMeta cat) {
		catalogs.add(cat);
	}

	String getColumnPartName(ColumnMeta column) throws ColumnNotFoundException,
			MutipleDefitionExeception {
		for (CatalogMeta cat : catalogs) {
			return cat.getColumnPartName(column);
		}
		return null;
	}
}

class CatalogMeta extends Description {
	List<SchemaMeta> schemas;

	void add(SchemaMeta schema) {
		schemas.add(schema);
	}

	String getColumnPartName(ColumnMeta column) throws ColumnNotFoundException,
			MutipleDefitionExeception {

		for (SchemaMeta schema : schemas) {
			return schema.getColumnPartName(column);
		}
		return null;
	}
}

class SchemaMeta extends Description {
	List<TableMeta> tables;

	void add(TableMeta tableInfo) {
		tables.add(tableInfo);
	}

	String getColumnPartName(ColumnMeta column) throws ColumnNotFoundException,
			MutipleDefitionExeception {
		int foundNum = 0;
		String fullColumnName = "";
		for (TableMeta tab : tables) {
			fullColumnName = tab.getColumnPartName(column);
			if (fullColumnName != null && !fullColumnName.equals(""))
				foundNum++;
		}
		if (foundNum < 1)
			throw new ColumnNotFoundException(column.name
					+ " not found under the schema : " + this.name);
		if (foundNum > 1)
			throw new MutipleDefitionExeception(column.name
					+ " have mutiple definition found under the schema : "
					+ this.name);
		return fullColumnName;
	}
}

class TableMeta extends Description {
	List<ColumnMeta> cols;

	void add(ColumnMeta col) {
		cols.add(col);
	}

	String getColumnPartName(ColumnMeta column) {
		for (ColumnMeta col : cols) {
			if (column.name.equals(col.name))
				return name + "." + column.name;
		}
		return null;
	}
}

class ColumnMeta extends Description {
	String colType;

	ColumnMeta(String colName) {
		this.name = colName;
	}
}

class Description {
	String remark;
	String name;
}

public class GDD {
	private String dbName;
	private int tableNum;
	private int siteNum;
	private Vector<TableInfo> tableInfos;
	private Vector<SiteInfo> siteInfos;
	
	AllSite allSite;
	// private Object SimpleExpression;

	public static final String CONTROL_SERVER_CONFIG = "config/gddserver.config";
	public static final String DATA_SERVER_CONFIG = "config/gdd.config";

	private GDD() {
		init();
	}

	private void init() {
		this.tableNum = 0;
		this.siteNum = 0;
		this.tableInfos = new Vector<TableInfo>();
		this.siteInfos = new Vector<SiteInfo>();
	}

	private GDD(String driverClass, String url, String user, String password)
			throws SQLException, ReflectiveOperationException {
		init();
		initData(ConnectionUtils
				.getConnection(driverClass, url, user, password));
	}

	private static GDD instance;

	public synchronized static GDD getInstance() {
		if (instance == null) {
			instance = new GDD();
		}
		return instance;
	}

	public synchronized static GDD getInstance(String driverClass, String url,
			String user, String password) throws SQLException,
			ReflectiveOperationException {
		if (instance == null) {
			instance = new GDD(driverClass, url, user, password);
		}
		return instance;
	}

	/**
	 * set Vector by metaData
	 * 
	 * @param connection
	 * @throws SQLException
	 * @throws ReflectiveOperationException
	 */
	private void initData(final Connection connection) throws SQLException,
			ReflectiveOperationException {
		final DatabaseMetaData database = connection.getMetaData();
		final MetadataContext context = new MetadataContext(database);

		initAllSite(context);
		initTableInfo(context);
		initSiteInfo(database);
	}

	private void initAllSite(final MetadataContext context)
			throws SQLException, ReflectiveOperationException {
		List<Catalog> catalogs = context.getCatalogs();
		for (Catalog catalog : catalogs) {
			CatalogMeta catalogMeta = new CatalogMeta();
			List<Schema> schemas = context.getSchemas(catalog.toString(), "*");
			for (Schema schema : schemas) {
				SchemaMeta schemaMeta = new SchemaMeta();
				List<Table> tables = context.getTables(catalog.toString(),
						schema.toString(), "*", new String[] {});
				for (Table table : tables) {
					TableMeta tableMeata = new TableMeta();
					List<Column> columns = context.getColumns(
							catalog.toString(), schema.toString(),
							table.toString(), "*");
					for (Column col : columns) {
						tableMeata.add(new ColumnMeta(col.getColumnName()));
					}
					schemaMeta.add(tableMeata);
				}
				catalogMeta.add(schemaMeta);
			}
			SiteMeta siteMeta = new SiteMeta();
			siteMeta.add(catalogMeta);
			allSite.add(siteMeta);
		}
	}

	private void initTableInfo(final MetadataContext context)
			throws SQLException, ReflectiveOperationException {
		List<Table> tables = context.getMetadata().getTables();
		tableInfos.addAll(TableConver.conver(tables));
	}

	private void initSiteInfo(final DatabaseMetaData database) {
		// TODO Auto-generated method stub

	}

	public BufferedReader getStringReader(String filepath) throws IOException,
			FileNotFoundException {
		BufferedReader StringReader = null;
		File file = new File(filepath);
		StringReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(file)));
		return StringReader;
	}

	public ColumnInfo readColumnInfo(BufferedReader br) {
		ColumnInfo columninfo = null;
		String str = null;
		int index;
		try {
			str = br.readLine();
			// System.out.println(str);
			if (str != null) {
				index = str.indexOf(":");
				str = str.substring(index + 1);

				Vector<String> strs = Utility.StringTokener(str);
				String name = strs.elementAt(0);
				int id = Integer.parseInt(strs.elementAt(1));
				int type = Integer.parseInt(strs.elementAt(2));
				int nullable = Integer.parseInt(strs.elementAt(3));
				int keyable = Integer.parseInt(strs.elementAt(4));
				int length = Integer.parseInt(strs.elementAt(5));
				columninfo = new ColumnInfo(name, id, type, nullable, keyable,
						length);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			System.out.println("nullpointexception here:" + str);
			e.printStackTrace();
		}
		return columninfo;
	}

	public FragmentationInfo readFragmentationInfo(BufferedReader br,
			int fragType) {
		FragmentationInfo fraginfo = null;
		FragmentationCondition fragCondition = null;
		int type = 0, expressSize = 0;
		String str = null;
		Vector<SimpleExpression> expressions = new Vector<SimpleExpression>();
		try {
			str = br.readLine();
			// System.out.println(str);
			if (str != null) {
				Vector<String> strs = Utility.StringTokener(str);
				String name = strs.elementAt(0);
				String condition = strs.elementAt(1);
				String sitename = strs.elementAt(2);
				int size = Integer.parseInt(strs.elementAt(3));
				fraginfo = new FragmentationInfo(fragType, name, condition,
						sitename, size);
			}

			str = br.readLine();
			if (str != null) {
				Vector<String> strs = Utility.StringTokener(str);
				type = Integer.parseInt(strs.elementAt(0));
				expressSize = Integer.parseInt(strs.elementAt(1));
			}

			if (type == CONSTANT.FRAG_HORIZONTAL) {
				for (int i = 0; i < expressSize; i++) {
					str = br.readLine();
					Vector<String> strs = Utility.StringTokener(str);
					SimpleExpression expression = new SimpleExpression(
							strs.elementAt(0), strs.elementAt(1),
							strs.elementAt(2), strs.elementAt(3),
							Integer.parseInt(strs.elementAt(4)));
					expressions.add(expression);
				}
				fragCondition = new FragmentationCondition(type, null,
						expressions);
				fraginfo.setFragmentationCondition(fragCondition);
			}

			if (type == CONSTANT.FRAG_VERTICAL) {
				Vector<String> strs = null;
				for (int i = 0; i < 1; i++) {
					str = br.readLine();
					strs = Utility.StringTokener(str);
				}
				fragCondition = new FragmentationCondition(type, strs, null);
				fraginfo.setFragmentationCondition(fragCondition);
			}

		} catch (IOException e) {
			System.out.println();
			e.printStackTrace();
			// System.exit(-1);
		} catch (NullPointerException e) {
			System.out.println("nullpointexception here:" + str);
			e.printStackTrace();
		}
		return fraginfo;
	}

	public TableInfo readTableInfo(BufferedReader br) {
		TableInfo tableinfo = null;
		String str = null;
		int index = 0;
		try {
			tableinfo = new TableInfo();

			str = br.readLine();
			while (str.length() == 0)
				str = br.readLine();

			String tablename = str;
			if (tablename != null)
				tableinfo.setTableName(tablename);

			str = br.readLine();
			// System.out.println(str);
			if (str != null) {
				index = str.indexOf(",");
				if (index != -1 && index != str.length()) {
					int id = Integer.parseInt(str.substring(0, index));
					int colnum = Integer.parseInt(str.substring(index + 1));
					tableinfo.setTableID(id);
					tableinfo.setColNum(colnum);
				}

				for (int i = 0; i < tableinfo.getColNum(); i++) {
					ColumnInfo columninfo = readColumnInfo(br);
					tableinfo.getColumnInfo().add(columninfo);
				}
			}

			str = br.readLine();
			// System.out.println(str);

			if (str != null) {
				index = str.indexOf(",");
				if (index != -1 && index != str.length()) {
					int fragtype = Integer.parseInt(str.substring(0, index));
					int fragnum = Integer.parseInt(str.substring(index + 1));
					tableinfo.setFragmentationType(fragtype);
					tableinfo.setFragmentationNum(fragnum);
				} else {

				}

				for (int i = 0; i < tableinfo.getFragNum(); i++) {
					FragmentationInfo fraginfo = readFragmentationInfo(br,
							tableinfo.getFragType());
					tableinfo.getFragmentationInfo().add(fraginfo);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			// System.exit(-1);
		}
		return tableinfo;
	}

	public SiteInfo readSiteInfo(BufferedReader br) {
		SiteInfo siteinfo = null;
		String str = null;
		try {
			int port = 0, num = 0;
			String ip = null;
			String name = null;
			Vector<String> fragNames = new Vector<String>();

			str = br.readLine();
			while (str.length() == 0)
				str = br.readLine();

			// System.out.println(str);
			if (str != null)
				name = str;
			if ((str = br.readLine()) != null)
				ip = str;
			if ((str = br.readLine()) != null)
				port = Integer.parseInt(str);
			if ((str = br.readLine()) != null)
				num = Integer.parseInt(str);
			if (num > 0) {
				if ((str = br.readLine()) != null) {
					fragNames = Utility.StringTokener(str, ",");
				}
			}
			siteinfo = new SiteInfo(name, ip, port, num, fragNames);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			System.out.println("nullpointexception here:" + str);
			e.printStackTrace();
		}
		return siteinfo;
	}

	public void GDDReader(String filepath) {
		this.tableNum = 0;
		this.siteNum = 0;
		this.tableInfos.clear();
		this.siteInfos.clear();
		String str;
		int i;
		try {

			File file = new File(filepath);
			if (!file.exists())
				return;

			BufferedReader br = getStringReader(filepath);

			if ((str = br.readLine()) != null) {
				// System.out.println(str);
				int index = str.indexOf(",");
				// System.out.println("index="+index);
				if (index != -1 && index != str.length()) {
					// System.out.println(str.substring(0,
					// index)+","+str.substring(index+1));
					this.tableNum = Integer.parseInt(str.substring(0, index));
					this.siteNum = Integer.parseInt(str.substring(index + 1));
				} else {
					System.out.println("file format error");
					return;
					// System.exit(-1);
				}
			}

			if (this.tableNum != 0) {
				for (i = 0; i < this.tableNum; i++) {
					str = br.readLine();
					TableInfo tableinfo = readTableInfo(br);
					if (tableinfo != null)
						this.tableInfos.add(tableinfo);
				}
			}

			if (this.siteNum != 0) {
				for (i = 0; i < this.siteNum; i++) {
					br.readLine();
					SiteInfo siteinfo = readSiteInfo(br);
					if (siteinfo != null)
						this.siteInfos.add(siteinfo);
				}
			}
			br.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void GDDWriter(String filepath) {
		FileOutputStream os;
		OutputStreamWriter or;
		BufferedWriter out = null;
		try {
			os = new FileOutputStream(filepath);
			or = new OutputStreamWriter(os);
			out = new BufferedWriter(or);
		} catch (IOException e) {
			System.out.println("File " + filepath
					+ "can not created or permission denied");
			System.exit(-1);
		}

		try {
			out.write(this.tableNum + "," + this.siteNum + "\n");
			for (int i = 0; i < this.tableInfos.size(); i++) {
				out.write("\n");
				TableInfo table = this.tableInfos.elementAt(i);
				out.write(table.getTableName() + "\n");
				out.write(table.getTabID() + "," + table.getColNum() + "\n");
				for (int j = 0; j < table.getColNum(); j++) {
					ColumnInfo column = table.getColumnInfo().elementAt(j);
					out.write("column" + (j + 1) + ":" + column.getColumnName()
							+ "," + column.getColumnID() + ","
							+ column.getColumnType() + ","
							+ column.getColumnNullable() + ","
							+ column.getColumnKeyable() + ","
							+ column.getColumnLength() + "\n");

				}
				out.write(table.getFragType() + "," + table.getFragNum() + "\n");
				for (int j = 0; j < table.getFragNum(); j++) {
					FragmentationInfo frag = table.getFragmentationInfo()
							.elementAt(j);
					out.write(frag.getFragName() + ","
							+ frag.getFragCondition() + ","
							+ frag.getFragSiteName() + "," + frag.getFragSize()
							+ "\n");
					FragmentationCondition condition = frag
							.getFragConditionExpression();
					int type = condition.fragmentationType;
					if (type == CONSTANT.FRAG_HORIZONTAL) {
						out.write(condition.fragmentationType
								+ ","
								+ condition.HorizontalFragmentationCondition
										.size() + "\n");
						for (int k = 0; k < condition.HorizontalFragmentationCondition
								.size(); k++) {
							SimpleExpression expression = condition.HorizontalFragmentationCondition
									.elementAt(k);
							out.write(expression.tableName);
							out.write("," + expression.columnName);
							out.write("," + expression.op);
							out.write("," + expression.value);
							out.write("," + expression.valueType);
							out.write("\n");
						}
					}
					if (type == CONSTANT.FRAG_VERTICAL) {
						out.write(condition.fragmentationType
								+ ","
								+ condition.verticalFragmentationCondition
										.size() + "\n");
						Vector<String> columns = condition.verticalFragmentationCondition;
						for (int k = 0; k < (columns.size() - 1); k++) {
							out.write(columns.elementAt(k) + ",");
						}
						out.write(columns.elementAt(columns.size() - 1) + "\n");
					}
				}
			}

			for (int i = 0; i < this.siteInfos.size(); i++) {
				out.write("\n");
				SiteInfo site = this.siteInfos.elementAt(i);
				out.write(site.getSiteName() + "\n");
				out.write(site.getSiteIP() + "\n");
				out.write(site.getSitePort() + "\n");
				out.write(site.getSiteFragNum() + "\n");
				if (site.getSiteFragNames().size() >= 1) {
					for (int j = 0; j < site.getSiteFragNum() - 1; j++) {
						out.write(site.getSiteFragNames().elementAt(j) + ",");
					}
					out.write(site.getSiteFragNames().elementAt(
							site.getSiteFragNum() - 1)
							+ "\n");
				}
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void setDBName(String db) {
		this.dbName = db;
	}

	public String getDBName() {
		return this.dbName;
	}

	/**
	 * is a table in the table_list?
	 * 
	 * @param tableName
	 *            String, table name
	 */
	public boolean isTableExist(String tableName) {
		int i;
		for (i = 0; i < tableInfos.size(); i++) {
			if (tableInfos.elementAt(i).getTableName()
					.equalsIgnoreCase(tableName))
				return true;
		}
		return false;
	}

	/**
	 * get a table from the table name.if the table isn't in the
	 * table_list,return null otherwise,return the tableInfo
	 * 
	 * @param tableName
	 *            String, table name
	 */
	public TableInfo getTableInfo(String tableName) {
		for (int i = 0; i < tableInfos.size(); i++) {
			if (tableInfos.elementAt(i).getTableName()
					.equalsIgnoreCase(tableName))
				return tableInfos.elementAt(i);
		}
		return null;
	}

	/**
	 * get the number of columns of a table,if the table isn't exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 */
	public int getTableColumnNum(String tableName) {
		if (!isTableExist(tableName))
			return -1;
		TableInfo tableinfo = getTableInfo(tableName);
		if (tableinfo != null)
			return tableinfo.getColNum();
		return -1;
	}

	/**
	 * is a column of a table exist?
	 * 
	 * @param tableName
	 *            String, table name
	 * @param columnName
	 *            String, column name
	 */
	public boolean isColumnExist(String tableName, String columnName) {
		if (!isTableExist(tableName))
			return false;
		TableInfo tableinfo = getTableInfo(tableName);
		for (int i = 0; i < tableinfo.getColNum(); i++) {
			if (tableinfo.getColumnInfo().elementAt(i).getColumnName()
					.equals(tableName))
				return true;
		}
		return false;
	}

	/**
	 * get a column of a table,if not exist,return null
	 * 
	 * @param tableName
	 *            String, table name
	 * @param columnName
	 *            String, column name
	 */
	public ColumnInfo getColumnInfo(String tableName, String columnName) {
		// if(!isColumnExist(tableName,columnName))
		// return null;
		TableInfo tableinfo = getTableInfo(tableName);
		if (tableinfo != null) {
			for (int i = 0; i < tableinfo.getColumnInfo().size(); i++) {
				if (tableinfo.getColumnInfo().elementAt(i).getColumnName()
						.equals(columnName))
					return tableinfo.getColumnInfo().elementAt(i);
			}
		}
		return null;
	}

	/**
	 * get a column's type,if the column isn't exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 * @param columnName
	 *            String, column name
	 */
	public int getColumnType(String tableName, String columnName) {
		ColumnInfo columninfo = getColumnInfo(tableName, columnName);
		if (columninfo == null)
			return -1;
		return columninfo.getColumnType();
	}

	/**
	 * get a column's length,if the column isn't exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 * @param columnName
	 *            String, column name
	 */
	public int getColumnLength(String tableName, String columnName) {
		ColumnInfo columninfo = getColumnInfo(tableName, columnName);
		if (columninfo == null)
			return -1;
		return columninfo.getColumnLength();
	}

	/**
	 * get a column's nullable,if the column isn't exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 * @param columnName
	 *            String, column name
	 */
	public int getColumnNullable(String tableName, String columnName) {
		ColumnInfo columninfo = getColumnInfo(tableName, columnName);
		if (columninfo == null)
			return -1;
		return columninfo.getColumnNullable();
	}

	/**
	 * get the number of fragmentation of a table,if the table isn't
	 * exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 */
	public int getTableFragmentationNum(String tableName) {
		if (!isTableExist(tableName))
			return -1;
		TableInfo tableinfo = getTableInfo(tableName);
		return tableinfo.getFragNum();
	}

	/**
	 * is a fragmentation of a table exist?
	 * 
	 * @param tableName
	 *            String, table name
	 * @param fragName
	 *            String, fragmentation name
	 */
	public boolean isFragmentationExist(String tableName, String fragName) {
		if (!isTableExist(tableName))
			return false;
		TableInfo tableinfo = getTableInfo(tableName);
		for (int i = 0; i < tableinfo.getFragNum(); i++) {
			if (tableinfo.getFragmentationInfo().elementAt(i).getFragName()
					.equals(fragName))
				return true;
		}
		return false;
	}

	/**
	 * get a fragmentation of a table,if not exist,return null
	 * 
	 * @param tableName
	 *            String, table name
	 * @param fragName
	 *            String, fragmentation name
	 */
	public FragmentationInfo getFragmentation(String tableName, String fragName) {
		if (!isTableExist(tableName))
			return null;
		TableInfo tableinfo = getTableInfo(tableName);
		for (int i = 0; i < tableinfo.getFragNum(); i++) {
			if (tableinfo.getFragmentationInfo().elementAt(i).getFragName()
					.equalsIgnoreCase(fragName))
				return tableinfo.getFragmentationInfo().elementAt(i);
		}
		return null;
	}

	public FragmentationInfo getFragmentation(String fragName) {
		for (int i = 0; i < this.tableInfos.size(); i++) {
			TableInfo tableinfo = this.tableInfos.get(i);
			for (int j = 0; j < tableinfo.getFragNum(); j++) {
				if (tableinfo.getFragmentationInfo().elementAt(j).getFragName()
						.equalsIgnoreCase(fragName))
					return tableinfo.getFragmentationInfo().elementAt(j);
			}
		}
		return null;
	}

	/**
	 * get a fragmentation's fragmentation_type,if the fragmentation isn't
	 * exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 * @param fragName
	 *            String, fragmentation name
	 */
	public int getFragmentationType(String tableName, String fragName) {
		FragmentationInfo fraginfo = getFragmentation(tableName, fragName);
		if (fraginfo == null)
			return -1;
		return fraginfo.getFragType();
	}

	/**
	 * get a fragmentation's allocation_site,if the fragmentation isn't
	 * exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 * @param fragName
	 *            String, fragmentation name
	 */
	public int getFragmentationSite(String tableName, String fragName) {
		FragmentationInfo fraginfo = getFragmentation(tableName, fragName);
		if (fraginfo == null)
			return -1;
		return fraginfo.getFragSiteNumber();
	}

	/**
	 * get a fragmentation's size,if the fragmentation isn't exist,return -1
	 * 
	 * @param tableName
	 *            String, table name
	 * @param fragName
	 *            String, fragmentation name
	 */
	public int getFragmentationSize(String tableName, String fragName) {
		FragmentationInfo fraginfo = getFragmentation(tableName, fragName);
		if (fraginfo == null)
			return -1;
		return fraginfo.getFragSize();
	}

	/**
	 * get a fragmentation's fragmentation_condition,if the fragmentation isn't
	 * exist,return null
	 * 
	 * @param tableName
	 *            String, table name
	 * @param fragName
	 *            String, fragmentation name
	 */
	public String getFragmentationCondition(String tableName, String fragName) {
		FragmentationInfo fraginfo = getFragmentation(tableName, fragName);
		if (fraginfo == null)
			return null;
		return fraginfo.getFragCondition();
	}

	/**
	 * is a site exist?
	 * 
	 * @param siteID
	 *            int, site's id
	 */
	public boolean isSiteExist(int siteID) {
		for (int i = 0; i < siteInfos.size(); i++) {
			if (siteInfos.elementAt(i).getSiteID() == siteID)
				return true;
		}
		return false;
	}

	/**
	 * get a SiteInfo
	 * 
	 * @param siteID
	 *            int, site's id
	 */
	public SiteInfo getSiteInfo(int siteID) {
		for (int i = 0; i < siteInfos.size(); i++) {
			if (siteInfos.elementAt(i).getSiteID() == siteID)
				return siteInfos.elementAt(i);
		}
		return null;
	}

	public SiteInfo getSiteInfo(String siteName) {
		for (int i = 0; i < siteInfos.size(); i++) {
			if (siteInfos.elementAt(i).getSiteName().equalsIgnoreCase(siteName))
				return siteInfos.elementAt(i);
		}
		return null;
	}

	/**
	 * get a site's ip
	 * 
	 * @param siteID
	 *            int, site's id
	 */
	public String getSiteIP(int siteID) {
		SiteInfo siteinfo = getSiteInfo(siteID);
		if (siteinfo == null)
			return null;
		return siteinfo.getSiteIP();
	}

	/**
	 * get a site's port
	 * 
	 * @param siteID
	 *            int, site's id
	 */
	public int getSitePort(int siteID) {
		SiteInfo siteinfo = getSiteInfo(siteID);
		if (siteinfo == null)
			return -1;
		return siteinfo.getSitePort();
	}

	/**
	 * get a site's fragmentation num
	 * 
	 * @param siteID
	 *            int, site's id
	 */
	public int getSiteFragNum(int siteID) {
		SiteInfo siteinfo = getSiteInfo(siteID);
		if (siteinfo == null)
			return -1;
		return siteinfo.getSiteFragNum();
	}

	/**
	 * get a site's fragmentation_list
	 * 
	 * @param siteID
	 *            int, site's id
	 */
	public Vector<String> getSiteFragNames(int siteID) {
		SiteInfo siteinfo = getSiteInfo(siteID);
		if (siteinfo == null)
			return null;
		return siteinfo.getSiteFragNames();
	}

	public Vector<SiteInfo> getSiteInfo() {
		return this.siteInfos;
	}

	public Vector<TableInfo> getTableInfos() {
		return this.tableInfos;
	}

	public void addSiteNum() {
		this.siteNum++;
	}

	public void addTableNum() {
		this.tableNum++;
	}

	public int getTableNum() {
		return this.tableNum;
	}

	public TableInfo getTableInfoFromFragName(String fragName) {
		for (int i = 0; i < this.tableInfos.size(); i++) {
			TableInfo tableinfo = this.tableInfos.get(i);
			for (int j = 0; j < tableinfo.getFragNum(); j++) {
				if (tableinfo.getFragmentationInfo().elementAt(j).getFragName()
						.equalsIgnoreCase(fragName))
					return tableinfo;
			}
		}
		return null;
	}

	public Vector<String> getTableFragList(String tableName) {
		Vector<String> tablelist = new Vector<String>();
		TableInfo tableinfo = this.getTableInfo(tableName);

		if (tableinfo == null)
			return null;

		Vector<FragmentationInfo> fraginfos = tableinfo.getFragmentationInfo();
		for (int i = 0; i < fraginfos.size(); i++)
			tablelist.add(fraginfos.elementAt(i).getFragName());
		return tablelist;
	}

	public String getSiteNameofFragmentation(String fragName) {
		FragmentationInfo fraginfo = this.getFragmentation(fragName);
		if (fraginfo == null) {
			return null;
		}
		return fraginfo.getFragSiteName();
	}

	public int getSiteNumberofFragmentation(String fragName) {
		FragmentationInfo fraginfo = this.getFragmentation(fragName);
		if (fraginfo == null) {
			return -1;
		}
		return fraginfo.getFragSiteNumber();
	}

	public String getTableNameofFragmentation(String fragName) {
		TableInfo tableinfo = this.getTableInfoFromFragName(fragName);
		if (tableinfo == null)
			return null;
		return tableinfo.getTableName();

	}

	public boolean isSameTableOfFrags(String fragName1, String fragName2) {
		String table1 = this.getTableNameofFragmentation(fragName1);
		String table2 = this.getTableNameofFragmentation(fragName2);

		if (table1 == null || table2 == null)
			return false;
		if (table1.equals(table2))
			return true;

		return false;
	}

	public Vector<ColumnInfo> getKeyColumns(String tableName) {
		TableInfo tableinfo = this.getTableInfo(tableName);
		if (tableinfo == null)
			return null;
		Vector<ColumnInfo> columnInfos = new Vector<ColumnInfo>();
		Vector<ColumnInfo> columns = tableinfo.getColumnInfo();
		for (int i = 0; i < columns.size(); i++) {
			if (columns.elementAt(i).getColumnKeyable() == 1)
				columnInfos.add(columns.elementAt(i));
		}
		return columnInfos;
	}

	public boolean isKeyOfTable(String tableName, String columnName) {
		TableInfo tableinfo = this.getTableInfo(tableName);
		if (tableinfo == null)
			return false;
		Vector<ColumnInfo> columns = tableinfo.getColumnInfo();
		for (int i = 0; i < columns.size(); i++) {
			if (columns.elementAt(i).getColumnName().equals(columnName)) {
				if (columns.elementAt(i).getColumnKeyable() == 1)
					return true;
			}
		}
		return false;
	}

	public int getTableSize(String tableName) {

		TableInfo tableinfo = this.getTableInfo(tableName);
		if (tableinfo != null) {
			Vector<FragmentationInfo> fragInfos = tableinfo
					.getFragmentationInfo();
			int size = 0;
			for (int i = 0; i < fragInfos.size(); i++) {
				size += fragInfos.elementAt(i).getFragSize();
			}
			return size;
		} else {
			FragmentationInfo fraginfo = this.getFragmentation(tableName);
			if (fraginfo != null) {
				return fraginfo.getFragSize();
			}
		}
		return 0;
	}

	public void printSiteFragmentation(int siteID) {
		Vector<String> names = getSiteFragNames(siteID);
		if (names != null) {
			for (int i = 0; i < names.size(); i++)
				System.out.println(names.elementAt(i));
		}
	}

	public void printSiteInfos() {
		for (int i = 0; i < this.siteInfos.size(); i++) {
			SiteInfo site = this.siteInfos.elementAt(i);
			site.printSite();
		}
	}

	public void printTableInfos() {
		for (int i = 0; i < this.tableInfos.size(); i++) {
			this.tableInfos.elementAt(i).printTableInfo();
		}
	}

	public void printGDD() {
		System.out.print("tableNum=" + this.tableNum);
		System.out.println(", siteNum=" + this.siteNum);
		this.printTableInfos();
		this.printSiteInfos();

	}

}