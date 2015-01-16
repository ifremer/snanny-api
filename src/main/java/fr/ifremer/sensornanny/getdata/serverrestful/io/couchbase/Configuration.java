package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Configuration {

	public static Configuration getInstance() {
		return instance;
	}
	
	private static final String CONFIGURATION_FILENAME = "couchbase.properties";
	
	private static final Logger logger = Logger.getLogger(Configuration.class.getName());
	private static Configuration instance = new Configuration();
	private Properties properties;
	
	private Configuration() {
		load();
	}
	
	public String[] cluster() {
		return get("cluster").split(",");
	}
	
	public String systemsBucket() {
		return get("systemsBucket");
	}

	public String observationsBucket() {
		return get("observationsBucket");
	}

	public String observationsViewDesign() {
		return get("observationsViewDesign");
	}
	
	public String observationsViewName() {
		return get("observationsViewName");
	}
	
	public String systemsViewDesign() {
		return get("systemsViewDesign");
	}
	
	public String systemsViewName() {
		return get("systemsViewName");
	}

	private void load() {
		InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILENAME);
		
		if (inputStream != null) {
			properties = new Properties();
			try {
				properties.load(inputStream);
			} catch (IOException e) {
				properties = null;
				logger.log(Level.SEVERE, "Error while reading '" + CONFIGURATION_FILENAME + "'", e);
				e.printStackTrace();
			}
		} else {
			logger.log(Level.SEVERE, "Property file '" + CONFIGURATION_FILENAME + "' not found in the classpath");
			throw new RuntimeException("Property file '" + CONFIGURATION_FILENAME + "' not found in the classpath");
		}
	}
	
	private void checkProperties() {
		if (properties == null) {
			logger.log(Level.SEVERE, "Property file '" + CONFIGURATION_FILENAME + "' not initialized");
			throw new RuntimeException("Property file '" + CONFIGURATION_FILENAME + "' not initialized");
		}
	}
	
	private String get(String property) {
		checkProperties();
		String value = properties.getProperty(property);
		if (value == null) {
			logger.log(Level.SEVERE, "Property named " + property + " not found in '" + CONFIGURATION_FILENAME + "'");
			throw new RuntimeException("Property named " + property + " not found in '" + CONFIGURATION_FILENAME + "'");
		}
		return value;
	}
	
	/*
	public static void main(String[] args) {
		System.out.println(Arrays.asList(Configuration.getInstance().cluster()));
		System.out.println(Configuration.getInstance().systemsBucket());
		System.out.println(Configuration.getInstance().observationsBucket());
		System.out.println(Configuration.getInstance().observationsViewDesign());
		System.out.println(Configuration.getInstance().observationsViewName());
		System.out.println(Configuration.getInstance().systemsViewDesign());
		System.out.println(Configuration.getInstance().systemsViewName());
	}
	*/
	
}
