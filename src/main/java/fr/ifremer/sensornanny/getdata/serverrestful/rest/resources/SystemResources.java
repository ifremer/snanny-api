package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.RequestStatuts;
import fr.ifremer.sensornanny.getdata.serverrestful.io.ObservationsSearch;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.logging.Logger;

import static fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields.*;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.EMPTY;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.STATUS_PROPERTY;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.SystemFields.*;

/**
 * Created by asi on 30/09/16.
 */
@Path("/system")
public class SystemResources {

    private static final Logger LOGGER = Logger.getLogger(SystemResources.class.getName());

    private ObservationsSearch elasticDb = new ObservationsSearch();

    private static final JsonParser parser = new JsonParser();

    /**
     * Pour les systèmes avec pour chacun, leur enfants et le lien vers le xml sensorML (dans owncloud-api).
     *
     * @param uuid {uuid}
     * @return
     */
    @GET
    @Path("{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getSystemsByUuid(@PathParam("uuid") String uuid) {
        JsonObject result = new JsonObject();
        long beginTime = System.currentTimeMillis();

        //special case : and uuid with no start & end date
        if(uuid.endsWith("__")){
            uuid = uuid.substring(0, uuid.length()-2);
        }

        final String sensorId = uuid;

        String field = SNANNY_ANCESTORS + "." + SNANNY_ANCESTOR_UUID;
        String filterField = SNANNY_ANCESTOR_UUID;

        if (uuid.contains("_")) {
            field = SNANNY_ANCESTORS + "." + SNANNY_ANCESTOR_DEPLOYMENTID;
            filterField = SNANNY_ANCESTOR_DEPLOYMENTID;
        }

        SearchResponse observations = elasticDb.getSystemsByField(uuid, field);

        JsonArray arr = new JsonArray();

        Long hits = observations.getHits().getTotalHits();
        if (hits == 0) {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
        } else {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());

            final String finalField = filterField;
            observations.getHits().forEach(hit -> createSystemObject(arr, hit, sensorId, finalField));

            result.add(SYSTEM_PROPERTY, arr);
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hits == null) ? "NaN" : hits.toString();
            LOGGER.info(String.format(
                    "Retrieve Systems :\n\tResult :{status: '%s', found: '%s', took '%dms'}",
                    result.get(STATUS_PROPERTY), numberOfHits, tookTime));

        }

        return result;
    }

    /**
     * Pour les systèmes avec pour chacun, leur enfants et le lien vers le xml sensorML (dans owncloud-api).
     *
     * @param uuid
     * @param startdate
     * @param enddate
     * @return
     */
    @GET
    @Path("deployment/{uuid}_{startdate}_{enddate}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getSystemsByDeploymentId(@PathParam("uuid") String uuid, @PathParam("startdate") String startdate, @PathParam("enddate") String enddate) {
        JsonObject result = new JsonObject();
        long beginTime = System.currentTimeMillis();

        String deploymentid = uuid + "_" + startdate + "_" + enddate;

        SearchResponse observations = elasticDb.getSystemsByField(deploymentid, SNANNY_ANCESTORS + "." + SNANNY_ANCESTOR_DEPLOYMENTID);

        JsonArray arr = new JsonArray();

        Long hits = observations.getHits().getTotalHits();
        if (hits == 0) {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
        } else {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());

            observations.getHits().forEach(hit -> createSystemObject(arr, hit, deploymentid, SNANNY_ANCESTOR_DEPLOYMENTID));

            result.add(SYSTEM_PROPERTY, arr);
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hits == null) ? "NaN" : hits.toString();
            LOGGER.info(String.format(
                    "Retrieve Systems :\n\tResult :{status: '%s', found: '%s', took '%dms'}",
                    result.get(STATUS_PROPERTY), numberOfHits, tookTime));

        }
        return result;
    }

    /**
     * Pour récuperer les systèmes indexés dans snanny-systems, avec la possibilité de filtrer par systèmes ayant des données ou non.
     * @param hasData
     * @return
     */
    @GET
    @Path("/all")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getSystemsWithData(@QueryParam("hasdata") String hasData) {
        JsonObject result = new JsonObject();
        long beginTime = System.currentTimeMillis();

        SearchResponse observations = elasticDb.getSystemsWithData(hasData);

        JsonArray arr = new JsonArray();

        Long hits = observations.getHits().getTotalHits();
        if (hits == 0) {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
        } else {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());

            observations.getHits().forEach(hit -> arr.add(parser.parse(hit.getSourceAsString()).getAsJsonObject()));

            result.add(SYSTEM_PROPERTY, arr);
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hits == null) ? "NaN" : hits.toString();
            LOGGER.info(String.format(
                    "Retrieve Systems :\n\tResult :{status: '%s', found: '%s', took '%dms'}",
                    result.get(STATUS_PROPERTY), numberOfHits, tookTime));

        }
        return result;
    }

    private void createSystemObject(JsonArray array, SearchHit hit, String id, String field) {

        JsonObject input = parser.parse(hit.getSourceAsString()).getAsJsonObject();

        int indexAncestor = -1;
        JsonArray ancestors = input.get(SNANNY_ANCESTORS).getAsJsonArray();
        for (int i = 0; i < ancestors.size(); i++) {
            JsonObject objectAncestor = (JsonObject) ancestors.get(i);
            if (extractVal(objectAncestor, field).equals(id)) {
                indexAncestor = i;
            }
            if (indexAncestor != -1 && i >= indexAncestor) {
                JsonObject ancestor = new JsonObject();
                ancestor.addProperty(SYSTEM_UUID, extractVal(objectAncestor, SNANNY_ANCESTOR_UUID));
                ancestor.addProperty(SYSTEM_DESCRIPTION, extractVal(objectAncestor, SNANNY_ANCESTOR_DESCRIPTION));
                ancestor.addProperty(SYSTEM_DEPLOYMENTID, extractVal(objectAncestor, SNANNY_ANCESTOR_DEPLOYMENTID));
                ancestor.addProperty(SYSTEM_NAME, extractVal(objectAncestor, SNANNY_ANCESTOR_NAME));
                ancestor.addProperty(SYSTEM_FILE, formatForJson(Config.smlEndpoint() + objectAncestor.get(SNANNY_ANCESTOR_UUID)));
                array.add(ancestor);
            }
        }
    }

    private String extractVal(JsonObject object, String id) {
        String value = formatForJson(object.get(id).toString());
        return value.equals("null") ? EMPTY : value;
    }

    private String formatForJson(String input) {
        return input.replaceAll("\"", "");
    }
}
