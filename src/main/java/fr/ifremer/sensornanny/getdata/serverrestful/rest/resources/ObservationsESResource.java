package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.constants.GeoConstants;
import fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields;
import fr.ifremer.sensornanny.getdata.serverrestful.converters.AggregatTimeConsumer;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.RequestStatuts;
import fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase.Configuration;
import fr.ifremer.sensornanny.getdata.serverrestful.io.elastic.ElasticConfiguration;
import fr.ifremer.sensornanny.getdata.serverrestful.io.elastic.ObservationsSearch;
import fr.ifremer.sensornanny.getdata.serverrestful.transform.GeoAggregatToGridTransformer;
import fr.ifremer.sensornanny.getdata.serverrestful.util.query.QueryResolver;

@Path(ObservationsESResource.PATH)
public class ObservationsESResource {

    private static final String SCROLL_PROPERTY = "scroll";

    private static final String STATUS_PROPERTY = "status";

    private static final String FEATURE_COLLECTION_VALUE = "FeatureCollection";

    private static final String FEATURES_PROPERTY = "features";

    private static final String TOTAL_COUNT_PROPERTY = "totalCount";

    private static final String TYPE_PROPERTY = "type";

    private static final Logger LOGGER = Logger.getLogger(ObservationsESResource.class.getName());

    private ObservationsSearch elasticDb = new ObservationsSearch();

    public static final String PATH = "/obs";

    /**
     * Get list of observations
     * 
     * @param bboxQuery geoBox with [FromLat,FromLon,ToLat,ToLon]
     * @param timeQuery timeQuery with [FromDate,ToDate]
     * @param keywordsQuery query with keywords elements (allow multi term : termA 2011,termB 2012
     * 
     * @return list of observation in JSON format
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getObservations(@QueryParam("bbox") String bboxQuery, @QueryParam("time") String timeQuery,
            @QueryParam("kwords") String keywordsQuery) {

        JsonObject result = JsonObject.create();
        ObservationQuery query = QueryResolver.resolveQueryObservation(bboxQuery, timeQuery, keywordsQuery);
        long beginTime = System.currentTimeMillis();
        Long hits = null;
        try {
            SearchResponse observations = elasticDb.getObservations(query);

            result.put(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);
            JsonArray arr = JsonArray.create();
            result.put(FEATURES_PROPERTY, arr);

            hits = observations.getHits().getTotalHits();
            result.put(TOTAL_COUNT_PROPERTY, hits);
            if (hits == 0) {
                result.put(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
            } else if (hits > ElasticConfiguration.aggregationLimit()) {
                result.put(STATUS_PROPERTY, RequestStatuts.TOOMANY.toString());
            } else {
                result.put(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());
                for (SearchHit searchHit : observations.getHits().hits()) {
                    arr.add(JsonObject.fromJson(searchHit.getSourceAsString()).get("doc"));
                }
                // Has more data
                if (arr.size() >= ElasticConfiguration.scrollPagination()) {
                    result.put(SCROLL_PROPERTY, observations.getScrollId());
                }
            }
        } catch (ElasticsearchException e) {
            // Query timeout exception
            result.put(STATUS_PROPERTY, RequestStatuts.TIMEOUT.toString());
        }

        if (Configuration.getInstance().individualDebug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hits == null) ? "NaN" : hits.toString();
            String scroll = result.getString(SCROLL_PROPERTY) != null ? result.getString(SCROLL_PROPERTY) : "No";
            LOGGER.info(String.format(
                    "Retrieve Observations using query : %s\n\tResult :{status: '%s', found: '%s', took '%dms', scroll:'%s'}",
                    query, result.get(STATUS_PROPERTY), numberOfHits, tookTime, scroll));

        }

        return result;

    }

    /**
     * Get list of observations from scroll
     * 
     * @param scrollQuery scrollId from a previous paging search
     * @return list of observation in JSON format
     */
    @GET
    @Path("scroll")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getObservations(@QueryParam("id") String scrollQuery) {

        JsonObject result = JsonObject.create();
        long beginTime = System.currentTimeMillis();
        Long hits = null;
        try {
            SearchResponse observations = elasticDb.getObservationsFromScroll(scrollQuery);

            JsonArray arr = JsonArray.create();

            result.put(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());
            result.put(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);

            hits = observations.getHits().totalHits();
            for (SearchHit searchHit : observations.getHits().hits()) {
                arr.add(JsonObject.fromJson(searchHit.getSourceAsString()).get("doc"));
            }
            // Has more data
            if (arr.size() >= ElasticConfiguration.scrollPagination()) {
                result.put(SCROLL_PROPERTY, observations.getScrollId());
            }
            result.put(FEATURES_PROPERTY, arr);
        } catch (ElasticsearchException e) {
            // Query timeout exception
            result.put(STATUS_PROPERTY, RequestStatuts.TIMEOUT.toString());
        }

        if (Configuration.getInstance().individualDebug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hits == null) ? "NaN" : hits.toString();
            String scroll = result.getString(SCROLL_PROPERTY) != null ? result.getString(SCROLL_PROPERTY) : "No";
            LOGGER.info(String.format(
                    "Retrieve Observations using query : %s\n\tResult :{status: '%s', found: '%s', took '%dms', scroll:'%s'}",
                    "[Scroll = " + scrollQuery + "]", result.get(STATUS_PROPERTY), numberOfHits, tookTime, scroll));

        }

        return result;
    }

    /**
     * Get list of observations
     * 
     * @param bboxQuery geoBox with [FromLat,FromLon,ToLat,ToLon]
     * @param timeQuery timeQuery with [FromDate,ToDate]
     * @param keywordsQuery query with keywords elements (allow multi term : termA 2011,termB 2012
     * 
     * @return List of shapes using geoJSON with counting documents for each geozone
     */
    @GET
    @Path("synthetic/map")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getObservationsMap(@QueryParam("bbox") String bboxQuery, @QueryParam("time") String timeQuery,
            @QueryParam("kwords") String keywordsQuery) {
        // Return empty element ne sera pas affiché
        JsonObject result = JsonObject.create();

        long beginTime = System.currentTimeMillis();
        ObservationQuery query = QueryResolver.resolveQueryObservation(bboxQuery, timeQuery, keywordsQuery);
        SearchResponse response = elasticDb.getMap(query);

        double lonDistance = GeoConstants.MAX_LON * 2;
        if (query.getTo() != null) {
            // getBouncing
            lonDistance = (GeoConstants.MAX_LON + query.getTo().getLon()) - (GeoConstants.MAX_LON + query.getFrom()
                    .getLon());
        }

        double subDivLat = lonDistance / ElasticConfiguration.syntheticViewBinElements();
        if (subDivLat < ElasticConfiguration.syntheticViewMinBinSize()) {
            subDivLat = ElasticConfiguration.syntheticViewMinBinSize();
        }
        long totalVisible = 0;
        // Get Geo Aggregats
        InternalFilter internalFilter = response.getAggregations().get(
                ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE);
        InternalGeoHashGrid geoAggregat = null;

        long totalHits = response.getHits().getTotalHits();
        if (internalFilter != null) {
            totalVisible = internalFilter.getDocCount();
            geoAggregat = internalFilter.getAggregations().get(ObservationsFields.AGGREGAT_GEOGRAPHIQUE);
        } else {
            totalVisible = totalHits;
            geoAggregat = response.getAggregations().get(ObservationsFields.AGGREGAT_GEOGRAPHIQUE);
        }

        result.put(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);
        result.put(TOTAL_COUNT_PROPERTY, totalVisible);
        JsonArray jsonArray = GeoAggregatToGridTransformer.toGeoJson(geoAggregat, subDivLat, totalHits, totalVisible);
        result.put(FEATURES_PROPERTY, jsonArray);

        if (Configuration.getInstance().individualDebug()) {
            LOGGER.info(String.format(
                    "Get Geohash using query : %s\n\tResult :{numberOfAggregats: '%s', totalVisible:'%d' took '%dms'}",
                    query, jsonArray.size(), totalVisible, System.currentTimeMillis() - beginTime));

        }
        return result;
    }

    /**
     * Get the timeline aggregation for specified request query
     * 
     * @param bboxQuery geoBox with [FromLat,FromLon,ToLat,ToLon]
     * @param keywordsQuery query with keywords elements (allow multi term : termA 2011,termB 2012
     * @return timeline aggregation with specified interval for query
     */
    @GET
    @Path("synthetic/timeline")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getObservationsTime(@QueryParam("bbox") String bboxQuery,
            @QueryParam("kwords") String keywordsQuery) {
        long beginTime = System.currentTimeMillis();
        ObservationQuery query = QueryResolver.resolveQueryObservation(bboxQuery, null, keywordsQuery);
        SearchResponse response = elasticDb.getTimeline(query);

        // Get Geo Aggregats
        InternalFilter internalFilter = response.getAggregations().get(
                ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE);

        InternalHistogram<InternalHistogram.Bucket> timeAggregat = null;
        if (internalFilter != null) {
            timeAggregat = internalFilter.getAggregations().get(ObservationsFields.AGGREGAT_DATE);
        } else {
            timeAggregat = response.getAggregations().get(ObservationsFields.AGGREGAT_DATE);
        }

        AggregatTimeConsumer aggregatTimeConsumer = new AggregatTimeConsumer();
        timeAggregat.getBuckets().forEach(aggregatTimeConsumer);
        if (Configuration.getInstance().individualDebug()) {
            LOGGER.info(String.format("Get Timeline using query : %s\n\tResult :{numberOfAggregats: '%s', took '%dms'}",
                    query, aggregatTimeConsumer.getResult().size(), System.currentTimeMillis() - beginTime));

        }

        return aggregatTimeConsumer.getResult();
    }

}
