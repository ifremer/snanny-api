package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;

import com.google.gson.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.io.NodeManager;

@Path("/info")
public class StatusResources {

    private static final String STATUS_PROPERTY = "status";
    private NodeManager nodeManager = new NodeManager();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getStatus() {
        JsonObject result = new JsonObject();
        try {
            // Indicates search exists
            new SearchRequestBuilder(nodeManager.getClient(), SearchAction.INSTANCE).setIndices(Config
                    .observationsIndex()).setSize(0).setTerminateAfter(1).get();
            result.addProperty(STATUS_PROPERTY, "success");
        } catch (ElasticsearchException es) {
            result.addProperty(STATUS_PROPERTY, "failure");
        }

        return result;
    }
}
