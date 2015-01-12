package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;

public class ConnectionManager implements ServletContextListener {

	static Cluster cluster;
	static Bucket systems;
	static Bucket observations;
	static Bucket observations_dev;

	private static final Logger logger = Logger.getLogger(ConnectionManager.class.getName());

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		logger.log(Level.INFO, "Connecting to Couchbase Cluster");
		// nodes.add(URI.create("http://134.246.144.131:8091/pools"));
		cluster = CouchbaseCluster.create("134.246.144.50", "134.246.144.131");
		systems = cluster.openBucket("snanny_systems");
		observations = cluster.openBucket("snanny_observations");
		observations_dev = cluster.openBucket("snanny_observations_dev");
	}

	public static JsonDocument getSystem(String id) {
		JsonDocument response = null;
		try {
			response = systems.get(id);
		} catch (NoSuchElementException e) {
			System.out.println("ERROR: No system with id: " + e.getMessage());
			e.printStackTrace();
		}
		return response;
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		logger.log(Level.INFO, "Disconnecting from Couchbase Cluster");
		cluster.disconnect();
	}

}