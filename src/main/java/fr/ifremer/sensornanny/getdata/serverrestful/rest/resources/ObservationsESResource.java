package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import java.util.function.Consumer;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.gson.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.constants.GeoConstants;
import fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields;
import fr.ifremer.sensornanny.getdata.serverrestful.converters.AggregatTimeConsumer;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.RequestStatuts;
import fr.ifremer.sensornanny.getdata.serverrestful.io.ObservationsSearch;
import fr.ifremer.sensornanny.getdata.serverrestful.transform.GeoAggregatToGridTransformer;
import fr.ifremer.sensornanny.getdata.serverrestful.util.query.QueryResolver;

import static fr.ifremer.sensornanny.getdata.serverrestful.constants.PropertiesFields.*;

@Path(ObservationsESResource.PATH)
public class ObservationsESResource {

    private static final double GEOHASH_7 = 0.153;
    private static final double GEOHASH_6 = 1.2;

    private static final double GEOHASH_5 = 5;

    private static final String GEOPOS_WITHOUT_COORDS = "0,200";

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

        JsonObject result = new JsonObject();
        ObservationQuery queryWithCoords = QueryResolver.resolveQueryObservation(bboxQuery, timeQuery, keywordsQuery);

        long beginTime = System.currentTimeMillis();
        Long hitsWithCoords = null;
        try {
            SearchResponse observations = elasticDb.getObservations(queryWithCoords, true);

            result.addProperty(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);
            JsonArray arr = new JsonArray();
            result.add(FEATURES_PROPERTY, arr);

            hitsWithCoords = observations.getHits().getTotalHits();

            result.addProperty(TOTAL_COUNT_PROPERTY, hitsWithCoords);
            if (hitsWithCoords == 0) {
                result.addProperty(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
            } else if (hitsWithCoords > Config.aggregationLimit()) {
                result.addProperty(STATUS_PROPERTY, RequestStatuts.TOOMANY.toString());
            } else {
                result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());
                arr.addAll(createObservationsResult(observations));
                // Has more data
                if (arr.size() >= Config.scrollPagination()) {
                    result.addProperty(SCROLL_PROPERTY, observations.getScrollId());
                }
            }
        } catch (ElasticsearchException e) {
            // Query timeout exception
            e.printStackTrace();
            result.addProperty(STATUS_PROPERTY, RequestStatuts.TIMEOUT.toString());
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hitsWithCoords == null) ? "NaN" : hitsWithCoords.toString();
            String scroll = result.get(SCROLL_PROPERTY) != null ? result.get(SCROLL_PROPERTY).getAsString() : "No";
            LOGGER.info(String.format(
                    "Retrieve Observations using query : %s\n\tResult :{status: '%s', found: '%s', took '%dms', scroll:'%s'}",
                    queryWithCoords, result.get(STATUS_PROPERTY), numberOfHits, tookTime, scroll));

        }

        return result;

    }


    /**
     * Get list of observations
     *
     * @param timeQuery timeQuery with [FromDate,ToDate]
     * @param keywordsQuery query with keywords elements (allow multi term : termA 2011,termB 2012
     *
     * @return list of observation in JSON format
     */
    @GET
    @Path("withoutgeo")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getObservationsWithoutData(@QueryParam("time") String timeQuery, @QueryParam("kwords") String keywordsQuery) {

        JsonObject result = new JsonObject();
        ObservationQuery queryWithoutCoords = QueryResolver.resolveQueryObservation(null, timeQuery, keywordsQuery);

        long beginTime = System.currentTimeMillis();
        Long hitsWithoutCoords = null;
        try {
            SearchResponse observationsWithoutCoords = elasticDb.getObservations(queryWithoutCoords, false);

            result.addProperty(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);
            JsonArray arr = new JsonArray();
            result.add(FEATURES_PROPERTY, arr);

            hitsWithoutCoords = observationsWithoutCoords.getHits().getTotalHits();

            result.addProperty(TOTAL_COUNT_PROPERTY, hitsWithoutCoords);
            if (hitsWithoutCoords == 0) {
                result.addProperty(STATUS_PROPERTY, RequestStatuts.EMPTY.toString());
            } else {
                result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());
                arr.addAll(createObservationsResult(observationsWithoutCoords));
                // Has more data
                if (arr.size() >= Config.scrollPagination()) {
                    result.addProperty(SCROLL_PROPERTY, observationsWithoutCoords.getScrollId());
                }
            }
        } catch (ElasticsearchException e) {
            // Query timeout exception
            e.printStackTrace();
            result.addProperty(STATUS_PROPERTY, RequestStatuts.TIMEOUT.toString());
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hitsWithoutCoords == null) ? "NaN" : hitsWithoutCoords.toString();
            String scroll = "No";
            if(result.get(SCROLL_PROPERTY) != null && !result.get(SCROLL_PROPERTY).isJsonNull()) {
                scroll = result.get(SCROLL_PROPERTY).getAsString();
            }
            LOGGER.info(String.format(
                    "Retrieve Observations using query : %s\n\tResult :{status: '%s', found: '%s', took '%dms', scroll:'%s'}",
                    queryWithoutCoords, result.get(STATUS_PROPERTY), numberOfHits, tookTime, scroll));
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

        JsonObject result = new JsonObject();
        long beginTime = System.currentTimeMillis();
        Long hits = null;
        try {
            SearchResponse observations = elasticDb.getObservationsFromScroll(scrollQuery);

            JsonArray arr = new JsonArray();

            result.addProperty(STATUS_PROPERTY, RequestStatuts.SUCCESS.toString());
            result.addProperty(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);
            JsonParser parser = new JsonParser();
            hits = observations.getHits().totalHits();
            for (SearchHit searchHit : observations.getHits().hits()) {
                arr.add(parser.parse(searchHit.getSourceAsString()).getAsJsonObject().get("doc"));
            }
            // Has more data
            if (arr.size() >= Config.scrollPagination()) {
                result.addProperty(SCROLL_PROPERTY, observations.getScrollId());
            }
            result.add(FEATURES_PROPERTY, arr);
        } catch (ElasticsearchException e) {
            // Query timeout exception
            result.addProperty(STATUS_PROPERTY, RequestStatuts.TIMEOUT.toString());
        }

        if (Config.debug()) {
            long tookTime = System.currentTimeMillis() - beginTime;
            String numberOfHits = (hits == null) ? "NaN" : hits.toString();
            String scroll = result.get(SCROLL_PROPERTY) != null ? result.get(SCROLL_PROPERTY).getAsString() : "No";
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
        JsonObject result = new JsonObject();

        long beginTime = System.currentTimeMillis();

        double lonDistance = GeoConstants.MAX_LON * 2;

        ObservationQuery query = QueryResolver.resolveQueryObservation(bboxQuery, timeQuery, keywordsQuery);

        if (query.getTo() != null) {
            // getBouncing
            lonDistance = (GeoConstants.MAX_LON + query.getTo().getLon()) - (GeoConstants.MAX_LON + query.getFrom()
                    .getLon());
        }
        // Calculate subdivision square
        double subDivLat = lonDistance / Config.syntheticViewBinElements();
        if (subDivLat < Config.syntheticViewMinBinSize()) {
            subDivLat = Config.syntheticViewMinBinSize();
        }

        int precision = getPrecision(subDivLat);

        SearchResponse response = elasticDb.getMap(query, precision);

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

        RequestStatuts status = (totalVisible < Config.aggregationLimit()) ? RequestStatuts.SUCCESS
                : RequestStatuts.TOOMANY;
        result.addProperty(STATUS_PROPERTY, status.toString());
        result.addProperty(TYPE_PROPERTY, FEATURE_COLLECTION_VALUE);
        result.addProperty(TOTAL_COUNT_PROPERTY, totalVisible);
        JsonArray jsonArray = GeoAggregatToGridTransformer.toGeoJson(geoAggregat, subDivLat, totalHits, totalVisible);
        result.add(FEATURES_PROPERTY, jsonArray);

        if (Config.debug()) {
            LOGGER.info(String.format(
                    "Get Geohash using query : %s\n\tResult :{numberOfAggregats: '%s', totalVisible:'%d' took '%dms'}",
                    query, jsonArray.size(), totalVisible, System.currentTimeMillis() - beginTime));

        }
        return result;
    }

    private int getPrecision(double subDivLat) {
        // Find the most optimized precision function of the subDivSize
        int precision = 5;
        double kilometers = subDivLat * 110;
        if (kilometers < GEOHASH_7) {
            precision = 8;
        } else if (kilometers < GEOHASH_6) {
            precision = 7;
        } else if (kilometers < GEOHASH_5) {
            precision = 6;
        }

        return precision;
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
            @QueryParam("kwords") String keywordsQuery, @QueryParam("hasCoords") Boolean hasCoords) {
        long beginTime = System.currentTimeMillis();
        ObservationQuery query = QueryResolver.resolveQueryObservation(bboxQuery, null, keywordsQuery);

        SearchResponse response = elasticDb.getTimeline(query, hasCoords);

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
        if (Config.debug()) {
            LOGGER.info(String.format("Get Timeline using query : %s\n\tResult :{numberOfAggregats: '%s', took '%dms'}",
                    query, aggregatTimeConsumer.getResult().size(), System.currentTimeMillis() - beginTime));

        }

        return aggregatTimeConsumer.getResult();
    }

    private JsonArray createObservationsResult(SearchResponse observations) {
        JsonArray arr = new JsonArray();
        JsonParser parser = new JsonParser();
        observations.getHits().forEach(new Consumer<SearchHit>() {

            @Override
            public void accept(SearchHit t) {

                JsonObject fromJson = parser.parse(t.getSourceAsString()).getAsJsonObject();
                JsonObject geometry = new JsonObject();
                JsonObject ret = new JsonObject();

                ret.add("properties", fromJson);
                ret.add("geometry", geometry);
                geometry.addProperty("type", "Point");
                JsonElement coord = fromJson.get("snanny-coordinates");
                JsonArray coordinatesArr = new JsonArray();
                String[] coords;
                if(!coord.isJsonNull()) {
                    String coordinates = coord.getAsString();
                    coords = coordinates.split(",");
                    coordinatesArr.add(new JsonPrimitive(coords[1]));
                    coordinatesArr.add(new JsonPrimitive(coords[0]));
                } else {
                    coords = GEOPOS_WITHOUT_COORDS.split(",");
                    coordinatesArr.add(new JsonPrimitive(coords[1]));
                    coordinatesArr.add(new JsonPrimitive(coords[0]));
                }
                geometry.add("coordinates", coordinatesArr);

                fromJson.remove("snanny-coordinates");

                arr.add(ret);

            }
        });

        return arr;
    }

}
