package configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import configuration.ConfigBean.ConfigType;

public class Configuration {
	private static final String CONFIG_FILE = "config.cfg";

	private static ConfigBean src, target;

	private static Configuration configuration;
	static {
		configuration = new Configuration();
	}

	public static Configuration getInstance() {
		initConfigBean();
		return configuration;
	}

	private static void initConfigBean() {
		Properties prop = new Properties();
		try {
			Configuration.parse(CONFIG_FILE);
		} catch (Exception ex) {
			System.out.println("Configuration init:" + ex.toString());
		}
		src = new ConfigBean(prop,ConfigType.SRC);
		target = new ConfigBean(prop,ConfigType.TARGET);
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

}