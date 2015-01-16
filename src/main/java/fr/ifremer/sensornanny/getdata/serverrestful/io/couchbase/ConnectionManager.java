package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class ConnectionManager implements ServletContextListener {

	static Cluster cluster;
	static Bucket systems;
	static Bucket observations;

	private static final Logger logger = Logger.getLogger(ConnectionManager.class.getName());

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		logger.log(Level.INFO, "Connecting to Couchbase Cluster");
		cluster = CouchbaseCluster.create(Configuration.getInstance().cluster());
		systems = cluster.openBucket(Configuration.getInstance().systemsBucket());
		observations = cluster.openBucket(Configuration.getInstance().observationsBucket());
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		logger.log(Level.INFO, "Disconnecting from Couchbase Cluster");
		cluster.disconnect();
	}

}