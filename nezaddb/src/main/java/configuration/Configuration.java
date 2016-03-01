package configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.culturegraph.mf.sql.connection.ConnectionUtils;

import com.github.jinahya.sql.database.metadata.bind.Catalog;
import com.github.jinahya.sql.database.metadata.bind.MetadataContext;

import configuration.ConfigBean.ConfigType;

public class Configuration {

	private static final String CONFIG_FILE = "config/config.cfg";

	private static final String SQL_SRC_FILE = "config/sql/src.sql";

	private static final String SQL_TARGET_FILE = "config/sql/target.sql";

	private static ConfigBean src, target;

	private static AllSite allSite;

	private static Configuration configuration;

	private Configuration() {
	}

	static {
		configuration = new Configuration();
		initConfigBean();
	}

	public static Configuration getInstance(String driverClass, String url, String user, String password)
			throws SQLException, ReflectiveOperationException {
//		initMetaInfo(ConnectionUtils.getConnection(driverClass, url, user, password).getMetaData());
		return configuration;
	}

	private static void initConfigBean() {
		Properties prop = new Properties();
		try {
			prop = parse(new PropertiesParser(CONFIG_FILE));
			prop.setProperty(ConfigBean.SRC_SQL, parse(new StringParser(SQL_SRC_FILE)));
			prop.setProperty(ConfigBean.TARGET_SQL, parse(new StringParser(SQL_TARGET_FILE)));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		src = new ConfigBean(prop, ConfigType.SRC);
		target = new ConfigBean(prop, ConfigType.TARGET);
	}

	private static <T> T parse(final Parserable<T> callback) {
		return new ParserCallback<T>() {
			@Override
			public T execute() {
				return callback.doParse();
			}
		}.execute();
	}

	interface ParserCallback<T> {
		T execute();
	}

	static class PropertiesParser implements Parserable<Properties> {
		String file ;

		public PropertiesParser(String configFile) {
			this.file = configFile;
		}

		@Override
		public Properties doParse() {
			Properties prop = new Properties();
			try {
				prop.load(getInputStream(file));
			} catch (IOException e) {
				e.printStackTrace();
			}
			return prop;
		}
	}

	static class StringParser implements Parserable<String> {
		String file;

		StringParser(String filePath) {
			this.file = filePath;
		}

		@Override
		public String doParse() {
			StringWriter strWriter = new StringWriter();
			try {
				IOUtils.copy(getInputStream(file), strWriter, "UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
			return strWriter.toString();
		}
	}

	private static InputStream getInputStream(String filePath) {
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		return classloader.getResourceAsStream(filePath);
	}

	interface Parserable<T> {
		T doParse();
	}

	private static void initMetaInfo(final DatabaseMetaData database)
			throws SQLException, ReflectiveOperationException {
		MetadataContext mc = new MetadataContext(database);
		List<Catalog> catalogs = mc.getCatalogs();
		if (catalogs.size() == 0) {
			setNullCatalog(database);
		}
	}

	public static List<TableMeta> setNullCatalog(final DatabaseMetaData mc)
			throws SQLException, ReflectiveOperationException {
		ResultSet tableRs = mc.getTables(null, null, null, new String[] { "TABLE" });
		List<TableMeta> tms = new LinkedList<TableMeta>();
		int maxCount = -1;
		try {
			while (tableRs.next()) {
				if (++maxCount > 100)
					break;
				String tableName = tableRs.getString("TABLE_NAME");
				TableMeta tm = new TableMeta(tableRs.getString("TABLE_CAT"), tableRs.getString("TABLE_SCHEM"),
						tableName);
				ResultSet colRs = null;
				try {
					colRs = mc.getColumns(null, null, tableName, null);
					while (colRs.next()) {
						tm.add(new ColumnMeta(colRs.getString("COLUMN_NAME"), colRs.getString("TYPE_NAME")));
					}
					tms.add(tm);
				} finally {
					if (colRs != null)
						colRs.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tableRs.close();
		}
		return tms;
	}

	public static Configuration getInstance() throws SQLException, ReflectiveOperationException {
		return getInstance(Configuration.src.getDriverClass(), Configuration.src.getConnecitonUrl(),
				Configuration.src.getUsername(), Configuration.src.getPassword());
	}

	public String getSrcSql() {
		return Configuration.src.getSql();
	}

	public String getTargetSql() {
		return Configuration.target.getSql();
	}
}