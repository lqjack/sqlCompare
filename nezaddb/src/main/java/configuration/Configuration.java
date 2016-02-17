package configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.culturegraph.mf.sql.connection.ConnectionUtils;

import com.github.jinahya.sql.database.metadata.bind.Catalog;
import com.github.jinahya.sql.database.metadata.bind.Column;
import com.github.jinahya.sql.database.metadata.bind.MetadataContext;
import com.github.jinahya.sql.database.metadata.bind.Schema;
import com.github.jinahya.sql.database.metadata.bind.Table;

import configuration.ConfigBean.ConfigType;

public class Configuration {
	
	private static final String CONFIG_FILE = "config.cfg";

	private static ConfigBean src, target;
	
	private static AllSite allSite;

	private static Configuration configuration;
	
	static {
		configuration = new Configuration();
	}

	public static Configuration getInstance(String driverClass, String url, String user, String password) throws SQLException, ReflectiveOperationException {
		initConfigBean();
		
		final DatabaseMetaData database = ConnectionUtils
				.getConnection(driverClass, url, user, password).getMetaData();
		initMetaInfo(new MetadataContext(database));
		return configuration;
	}

	private static void initConfigBean() {
		Properties prop = new Properties();
		try {
			Configuration.parse(CONFIG_FILE);
		} catch (Exception ex) {
			System.out.println("Configuration init:" + ex.toString());
		}
		src = new ConfigBean(prop, ConfigType.SRC);
		target = new ConfigBean(prop, ConfigType.TARGET);
	}

	private static Properties parse(String file) {
		Properties prop = new Properties();
		ClassLoader classloader = Thread.currentThread()
				.getContextClassLoader();
		InputStream is = classloader.getResourceAsStream(file);
		try {
			prop.load(is);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}

	private static void initMetaInfo(final MetadataContext context)
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
}