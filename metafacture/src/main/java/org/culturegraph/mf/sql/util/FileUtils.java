package org.culturegraph.mf.sql.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

public class FileUtils {
	
	private static final String FILE_XML = "xml";

	public static Properties readConfigFile(String path) throws FileNotFoundException{
		File file = new File(path);
		if(!file.exists()){
			throw new FileNotFoundException(" file cannot be found by file path :" + path);
		}
		return loadPropertiesFromFilePath(file);
	}

	private static Properties loadPropertiesFromFilePath(File file) {
		Properties prop = new Properties();
		String fileName = file.getName();
		String fileFullPath = null;
		try {
			fileFullPath = file.getCanonicalPath();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			if(FILE_XML.equals(fileName.substring(fileName.lastIndexOf(".")))){
				prop.loadFromXML(new FileInputStream(fileFullPath));
			}else 
				prop.load(new FileInputStream(fileFullPath));
		} catch (InvalidPropertiesFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}
}
