package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import com.google.gson.*;
import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.RequestStatuts;
import fr.ifremer.sensornanny.getdata.serverrestful.io.ObservationsSearch;
import fr.ifremer.sensornanny.getdata.serverrestful.util.query.QueryResolver;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields.*;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.EMPTY;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.STATUS_PROPERTY;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.SystemFields.*;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.SystemFields.SYSTEM_NAME;

/**
 * Created by asi on 30/09/16.
 */
@Path("/systems")
public class SystemResources {

    private static final Logger LOGGER = Logger.getLogger(SystemResources.class.getName());
    private static final String ANCESTORFILE = "snanny-ancestor-file";
    private static final String DESCRIPTION = "description";
    private static final String TERMS = "terms";

    private ObservationsSearch elasticDb = new ObservationsSearch();

    private static final JsonParser parser = new JsonParser();

    /**
     * Pour la liste, la liste peut prendre des critères spatio-temporelles, keywords qui s'applique sur les données émises par les systèmes où leurs enfants.
     * @return
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getSystems(@QueryParam("bbox") String bboxQuery, @QueryParam("time") String timeQuery, @QueryParam("kwords") String keywordsQuery) {

        JsonObject result = new JsonObject();
        ObservationQuery query = QueryResolver.resolveQueryObservation(bboxQuery, timeQuery, keywordsQuery);
        long beginTime = System.currentTimeMillis();

        JsonArray arr = new JsonArray();
        SearchResponse observations = elasticDb.getSystems(query);

        int nbSystems = observations.getAggregations().asList().size();
        if(nbSystems == 0) {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
        } else {
            result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());
            StringTerms terms = (StringTerms) observations.getAggregations().get(AGGREGAT).getProperty(AGGREGAT_TERM);
            terms.getBuckets().forEach(new Consumer<Terms.Bucket>() {
                @Override
                public void accept(Terms.Bucket bucket) {

                    JsonObject system = new JsonObject();
                    String uuid = bucket.getKeyAsString();
                    system.addProperty(SYSTEM_UUID, uuid);
                    system.addProperty(SYSTEM_DEPLOYMENTID, getSignificantValue(bucket, SNANNY_ANCESTOR_DEPLOYMENTID));
                    system.addProperty(SYSTEM_NAME, getSignificantValue(bucket, SNANNY_ANCESTOR_NAME));
                    system.addProperty(SYSTEM_DESCRIPTION, getSignificantValue(bucket, SNANNY_ANCESTOR_DESCRIPTION));
                    system.addProperty(SYSTEM_TERMS, getSignificantValue(bucket, SNANNY_ANCESTOR_TERMS));
                    system.addProperty(SYSTEM_FILE, formatForJson(Config.smlEndpoint() + uuid));

                    arr.add(system);
                }
            });

            result.add(SYSTEM_PROPERTY, arr);
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            LOGGER.info(String.format(
                    "Retrieve Systems using query : %s\n\tResult :{status: '%s', found: '%s', took '%dms'}",
                    query, result.get(STATUS_PROPERTY), nbSystems, tookTime));

        }

        return result;
    }

    /**
     * Pour les systèmes avec pour chacun, leur enfants et le lien vers le xml sensorML (dans owncloud-api).
     * @param uuid {uuid}
     * @return
     */
    @GET
    @Path("{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getSystemsByUuid(@PathParam("uuid") String uuid) {
        JsonObject result = new JsonObject();
        long beginTime = System.currentTimeMillis();

        String field = SNANNY_ANCESTORS + "." + SNANNY_ANCESTOR_UUID;
        String filterField = SNANNY_ANCESTOR_UUID;

        if(uuid.contains("_")) {
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
            observations.getHits().forEach(hit -> createSystemObject(arr, hit, uuid, finalField));

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
     * @param uuid
     * @param startdate
     * @param enddate
     * @return
     */
    @GET
    @Path("{uuid}_{startdate}_{enddate}")
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
        for(int i=0; i < ancestors.size(); i++) {
            JsonObject objectAncestor = (JsonObject) ancestors.get(i);
            if(extractVal(objectAncestor, field).equals(id)) {
                indexAncestor = i;
            }
            if(indexAncestor != -1 && i >= indexAncestor) {
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

    private String getSignificantValue(Terms.Bucket bucket, String id) {
        List<Terms.Bucket> buckets = ((StringTerms) bucket.getAggregations().getProperty(id)).getBuckets();
        return buckets.size() > 0 ? formatForJson(buckets.get(0).getKeyAsString()) : EMPTY;
    }

    private String extractVal(JsonObject object, String id) {
        String value = formatForJson(object.get(id).toString());
        return value.equals("null") ? EMPTY : value;
    }

    private String formatForJson(String input) {
        return input.replaceAll("\"", "");
    }
}
