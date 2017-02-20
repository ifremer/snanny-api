package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.RequestStatuts;
import fr.ifremer.sensornanny.getdata.serverrestful.io.ObservationsSearch;
import fr.ifremer.sensornanny.getdata.serverrestful.util.query.QueryResolver;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields.*;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.EMPTY;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.STATUS_PROPERTY;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.SystemFields.*;

/**
 * Created by asi on 30/09/16.
 */
@Path("/systems")
public class SystemsResources {

    private static final Logger LOGGER = Logger.getLogger(SystemsResources.class.getName());
    private ObservationsSearch elasticDb = new ObservationsSearch();


    /**
     * Pour la liste, la liste peut prendre des critères spatio-temporelles, keywords qui s'applique sur les données émises par les systèmes où leurs enfants.
     *
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
        if (nbSystems == 0) {
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


    private String getSignificantValue(Terms.Bucket bucket, String id) {
        List<Terms.Bucket> buckets = ((StringTerms) bucket.getAggregations().getProperty(id)).getBuckets();
        return buckets.size() > 0 ? formatForJson(buckets.get(0).getKeyAsString()) : EMPTY;
    }

    private String formatForJson(String input) {
        return input.replaceAll("\"", "");
    }
}
