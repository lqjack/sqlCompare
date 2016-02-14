package configuration;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.Properties;

import org.culturegraph.mf.sql.connection.ConnectionUtils;

import utils.ReflectUtils;

/**
 * 
 * must not add static field 
 * 
 * @author a601240
 *
 */
public class ConfigBean {

	private static String SRC_DRIVER_CLASS = "src.driver.class";
	private static String SRC_CONNECTION_URL = "src.conneciton.url";
	private static String SRC_UERNAME = "src.username";
	private static String SRC_PASSWORD = "src.password";
	private static String SRC_SCHEMA = "src.schema";
	private static String SRC_CATELOG = "src.catelog";
	private static String SRC_TABLE_NAME = "src.table";
	private static String SRC_COLUMNS = "src.columns";
	private static String SRC_SQL = "src.sql";

	private static String TARGET_DRIVER_CLASS = "target.driver.class";
	private static String TARGET_CONNECTION_URL = "target.conneciton.url";
	private static String TARGET_USERNAME = "target.username";
	private static String TARGET_PASSWORD = "target.password";
	private static String TARGET_SCHEMA = "target.schema";
	private static String TARGET_CATALOG = "target.catelog";
	private static String TARGET_TABLE_NAME = "target.table";
	private static String TARGET_COLUMNS = "target.columns";
	private static String TARGET_SQL = "target.sql";

	private String PRE_SRC = "src.";
	private String PRE_TARGET = "target.";

	private String driverClass;
	private String connecitonUrl;
	private String username;
	private String password;
	private String schema;
	private String catelog;
	private String table;
	private String columns;

	private String sql;
	private ConfigType type;
	
	private String[] primaryKeys;
	
	private Properties prop;

	enum ConfigType {
		SRC, TARGET
	}

	private Connection conn;

	ConfigBean(Properties prop, ConfigType type) {
		this.type = type;
		this.prop = prop;
		setFields();
		initConnection();
	}

	private void initConnection() {
		conn = ConnectionUtils.getConnection(driverClass, connecitonUrl,
				username, password);
	}

	private void setFields() {
		Field[] declaredFields = ConfigBean.class.getDeclaredFields();
		for (Field field : declaredFields) {
			if (Modifier.isStatic(field.getModifiers())) {
				field.setAccessible(true);
				setValue(field);
			}
		}
	}

	private void setValue(Field field) {
		String fieldValue = ReflectUtils.getFieldValue(field);
		String fieldName = transform(fieldValue);
		ReflectUtils.setFieldValue(ReflectUtils.getFieldByFiledName(fieldName),
				this, (String)prop.get(fieldValue));
	}

	private String transform(String fieldValue) {
		if (fieldValue.contains(PRE_SRC)) {
			fieldValue = fieldValue.substring(PRE_SRC.length());
		} else {
			fieldValue = fieldValue.substring(PRE_TARGET.length());
		}
		String[] subFieldValue = fieldValue.split("\\.");
		if (subFieldValue.length < 1)
			throw new IndexOutOfBoundsException();
		StringBuffer filedBuffer = new StringBuffer(subFieldValue[0]);
		for (int i = 1; i < subFieldValue.length; i++) {
			String temp = subFieldValue[i];
			filedBuffer.append(((String) temp.subSequence(0, i)).toUpperCase())
					.append(temp.substring(1));
		}
		return filedBuffer.toString();
	}
	
	public String getSchema() {
		return schema;
	}

	public String getCatelog() {
		return catelog;
	}

	public String getTable() {
		return table;
	}

	public String getColumns() {
		return columns;
	}

	public String getSql() {
		return sql;
	}

	public ConfigType getType() {
		return type;
	}

	public String[] getPrimaryKeys() {
		return primaryKeys;
	}
}
