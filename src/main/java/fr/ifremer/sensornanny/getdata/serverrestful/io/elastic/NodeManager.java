package fr.ifremer.sensornanny.getdata.serverrestful.io.elastic;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * NodeManager elasticsearch, allow to create a transportClient on the defined
 * clusters
 * 
 * @author athorel
 *
 */
public class NodeManager implements ServletContextListener {

    private static final String CLIENT_TRANSPORT_SNIFF = "client.transport.sniff";

    private static final String CLUSTER_NAME = "cluster.name";

    private static final int ELASTICSEARCH_TRANSPORT_PORT = 9300;

    private static final Logger LOGGER = Logger.getLogger(NodeManager.class.getName());

    private static Settings clientSettings;

    private static TransportClient client;

    public NodeManager() {
        extractClientSettings();
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        LOGGER.log(Level.INFO, "Connecting to ElasticSearch Cluster");
        extractClientSettings();
    }

    private void extractClientSettings() {
        if (clientSettings == null) {
            clientSettings = ImmutableSettings.settingsBuilder().put(CLUSTER_NAME, ElasticConfiguration.clusterName())
                    .put(CLIENT_TRANSPORT_SNIFF, true).build();

            client = new TransportClient(clientSettings);
            String[] nodes = ElasticConfiguration.clusterHosts();
            for (String host : nodes) {
                client.addTransportAddress(new InetSocketTransportAddress(host, ELASTICSEARCH_TRANSPORT_PORT));
            }
        }
    }

    /**
     * Get the transport client for searchs
     * 
     * @return transport client configured with properties
     */
    public Client getClient() {
        LOGGER.log(Level.INFO, "GetClient " + client);
        return client;
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (client != null) {
            client.close();
        }
    }

}
