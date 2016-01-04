package fr.ifremer.sensornanny.getdata.serverrestful;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import fr.ifremer.sensornanny.getdata.serverrestful.util.properties.PropertyLoader;

/**
 * Configuation class for elastic search
 */
public class Config {

    /** Property file */
    private static final String CONFIGURATION_FILENAME = "application.properties";

    private static final Logger logger = Logger.getLogger(Config.class.getName());

    private static Config instance = new Config();

    private Properties properties;

    private Config() {
        properties = new Properties();
        PropertyLoader.load(CONFIGURATION_FILENAME, properties);
    }

    /**
     * Hosts of the nodes in a elasticSearch cluster
     * 
     * @return array of hosts
     */
    public static String[] clusterHosts() {
        return get("es.cluster.nodes").split(",");
    }

    /***
     * Name of the cluster elasticSearch (this configuration allow autodiscover nodes)
     * 
     * @return name of the cluster
     */
    public static String clusterName() {
        return get("es.cluster.name");
    }

    /**
     * Transport port of the cluster (default is 9300)
     * 
     * @return transport port of the nodes
     */
    public static String clusterPort() {
        return get("es.cluster.port");
    }

    /**
     * Index elastic search on observations
     * 
     * @return indexName
     */
    public static String observationsIndex() {
        return get("es.index.observations");
    }

    /**
     * Time in millis while elastic scroll index is kept alive
     * See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
     * 
     * @return keepalive time scroll in millis
     */
    public static String scrollTimeout() {
        return get("es.scroll.timeout");
    }

    /**
     * Number of items per page for elastic pagination
     * See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
     * 
     * @return number of items per page (ex : 10000)
     */
    public static int scrollPagination() {
        return getInt("es.scroll.itemsPerPage");
    }

    /**
     * Number of documents allowed to print (in result search is over, only the aggregate page is shown)
     * 
     * @return aggregation limit
     */
    public static int aggregationLimit() {
        return getInt("es.aggs.limit");
    }

    /**
     * Max time for a elasticSearch request allowed, if execution time exceed the timeout, an exception is raised, and
     * the search is cancelled
     * 
     * @return max time allowed for a search request
     */
    public static int queryTimeout() {
        return getInt("es.query.timeout");
    }

    public static double syntheticViewMinBinSize() {
        return getDouble("es.syntheticViewMinBinSize");
    }

    public static int syntheticViewBinElements() {
        return getInt("es.syntheticViewBinElements");
    }

    public static long syntheticTimelineMinDate() {
        return getLong("es.syntheticTimelineMinDate");
    }

    public static boolean debug() {
        return getBoolean("debug");
    }

    private void checkProperties() {
        if (properties == null) {
            String message = "Property file '" + CONFIGURATION_FILENAME + "' not initialized";
            logger.log(Level.SEVERE, message);
            throw new IllegalStateException(message);
        }
    }

    private static String get(String property) {
        instance.checkProperties();
        String value = instance.properties.getProperty(property);
        if (value == null) {
            String message = "Property named " + property + " not found in '" + CONFIGURATION_FILENAME
                    + "'";
            logger.log(Level.SEVERE, message);
            throw new IllegalStateException(message);
        }
        return value;
    }

    private static int getInt(String property) {
        return Integer.parseInt(get(property));
    }

    private static double getDouble(String property) {
        return Double.parseDouble(get(property));
    }

    private static long getLong(String property) {
        return Long.parseLong(get(property));
    }

    private static boolean getBoolean(String property) {
        return Boolean.parseBoolean(get(property));
    }

}