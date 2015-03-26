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
	
	private static final String CONFIGURATION_COUCHBASE_FILENAME = "couchbase.properties";
	private static final String CONFIGURATION_SYNTHETIC_FILENAME = "synthetic.properties";
	private static final String CONFIGURATION_INDIVIDUAL_FILENAME = "individual.properties";
	
	private static final Logger logger = Logger.getLogger(Configuration.class.getName());
	private static Configuration instance = new Configuration();
	private Properties properties;
	
	private Configuration() {
		load(CONFIGURATION_COUCHBASE_FILENAME);
		load(CONFIGURATION_SYNTHETIC_FILENAME);
		load(CONFIGURATION_INDIVIDUAL_FILENAME);
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
	
	public String systemsViewDesign() {
		return get("systemsViewDesign");
	}
	
	public String systemsViewName() {
		return get("systemsViewName");
	}

	public String observationsSyntheticViewDesign() {
		return get("observationsSyntheticViewDesign");
	}
	
	public String observationsSyntheticGeoTemporalCountViewName() {
		return get("observationsSyntheticGeoTemporalCountViewName");
	}

	public String observationsIndividualViewDesign() {
		return get("observationsIndividualViewDesign");
	}
	
	public String observationsIndividualGeoTemporalViewName() {
		return get("observationsIndividualGeoTemporalViewName");
	}
	
	public double syntheticLatitudeBinSize() {
		return Double.parseDouble(get("syntheticLatitudeBinSize"));
	}

	public double syntheticLongitudeBinSize() {
		return Double.parseDouble(get("syntheticLongitudeBinSize"));
	}
	
	public long syntheticTimeBinCoeff() {
		return Long.parseLong(get("syntheticTimeBinCoeff"));
	}
	
	public long syntheticTimeBinSize() {
		return Long.parseLong(get("syntheticTimeBinSize"));
	}
	
	public boolean syntheticViewLoadOnStartup() {
		return Boolean.parseBoolean(get("syntheticViewLoadOnStartup"));
	}
	
	public boolean individualDebug() {
		return Boolean.parseBoolean(get("individualDebug"));
	}
	
	public int individualQueryTimeout() {
		return Integer.parseInt(get("individualQueryTimeout"));
	}

	public int individualCollectTimeout() {
		return Integer.parseInt(get("individualCollectTimeout"));
	}

	public int individualQueryMaximumDocuments() {
		return Integer.parseInt(get("individualQueryMaximumDocuments"));
	}
	
	public double individualMaximumLatitudeRange() {
		return Double.parseDouble(get("individualMaximumLatitudeRange"));
	}

	public double individualMaximumLongitudeRange() {
		return Double.parseDouble(get("individualMaximumLongitudeRange"));
	}

	private void load(final String propertyFile) {
		InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(propertyFile);
		
		if (inputStream != null) {
			try {
				if (properties == null) {
					properties = new Properties();
				}
				properties.load(inputStream);
			} catch (IOException e) {
				properties = null;
				logger.log(Level.SEVERE, "Error while reading '" + propertyFile + "'", e);
			}
		} else {
			logger.log(Level.SEVERE, "Property file '" + propertyFile + "' not found in the classpath");
			throw new RuntimeException("Property file '" + propertyFile + "' not found in the classpath");
		}
	}
	
	private void checkProperties() {
		if (properties == null) {
			logger.log(Level.SEVERE, "Property file '" + CONFIGURATION_COUCHBASE_FILENAME + "' and '" + CONFIGURATION_SYNTHETIC_FILENAME + "' not initialized");
			throw new RuntimeException("Property file '" + CONFIGURATION_COUCHBASE_FILENAME + "' and '" + CONFIGURATION_SYNTHETIC_FILENAME + "' not initialized");
		}
	}
	
	private String get(String property) {
		checkProperties();
		String value = properties.getProperty(property);
		if (value == null) {
			logger.log(Level.SEVERE, "Property named " + property + " not found in '" + CONFIGURATION_COUCHBASE_FILENAME + "' nor '" + CONFIGURATION_SYNTHETIC_FILENAME + "'");
			throw new RuntimeException("Property named " + property + " not found in '" + CONFIGURATION_COUCHBASE_FILENAME + "' nor '" + CONFIGURATION_SYNTHETIC_FILENAME + "'");
		}
		return value;
	}
	
}
