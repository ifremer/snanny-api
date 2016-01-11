package fr.ifremer.sensornanny.getdata.serverrestful.io;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder.Operator;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields;
import fr.ifremer.sensornanny.getdata.serverrestful.context.CurrentUserProvider;
import fr.ifremer.sensornanny.getdata.serverrestful.context.User;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;

/**
 * This class allow access to elasticsearch observations databases
 * 
 * @author athorel
 *
 */
public class ObservationsSearch {

    private static final int PUBLIC_ACCESS_TYPE = 2;

    private static final String[] EXCLUDE_OBSERVATION_FIELDS = new String[] { "meta.*" };

    private static final String[] EXPORT_OBSERVATIONS_FIELDS = new String[] { "doc.snanny-uuid",
            "doc.snanny-ancestors.snanny-ancestor-name", "doc.snanny-ancestors.snanny-ancestor-uuid", "doc.snanny-name",
            "doc.snanny-coordinates" };

    private static final String STANDARD_TOKEN_ANALYZER = "standard";

    private static final int TIME_INTERVAL = 15 * 24 * 60 * 60 * 1000;

    private NodeManager nodeManager = new NodeManager();

    /**
     * Get a page of observations from a query
     * 
     * @param query geoboxing, timeboxing and keywords parameters
     * @return response containing the first page of observations using #ElasticConfiguration.scrollPagination()
     */
    public SearchResponse getObservations(ObservationQuery query) {

        SearchRequestBuilder searchRequest = createQuery(query);

        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter
            GeoBoundingBoxFilterBuilder geoFilter = FilterBuilders.geoBoundingBoxFilter(ObservationsFields.COORDINATES)
                    .bottomLeft(query.getFrom().geohash()).topRight(query.getTo().geohash());

            // Add Range data
            searchRequest.setPostFilter(geoFilter);
        }
        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        searchRequest.setFetchSource(EXPORT_OBSERVATIONS_FIELDS, EXCLUDE_OBSERVATION_FIELDS);

        // searchRequest.setScroll(ElasticConfiguration.scrollTimeout());
        return searchRequest.setFrom(0).setSize(Config.aggregationLimit()).execute().actionGet(Config.queryTimeout(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Get a page of observations from a scrollId, keep the query parameters and request the next X elements from the
     * last cursor
     * 
     * @param scrollId scroll identifier
     * @return response containing the X next elements
     */
    public SearchResponse getObservationsFromScroll(String scrollId) {

        Client client = nodeManager.getClient();
        // Connect on indice
        return client.prepareSearchScroll(scrollId).setScroll(Config.scrollTimeout()).execute().actionGet(Config
                .queryTimeout(), TimeUnit.MILLISECONDS);
    }

    /**
     * Get GeoJSON object representing ths aggregations for specific observation query
     * 
     * @param query search in term, time, and geobox
     * 
     * @return aggregations of documents limited by the query in zones
     */
    public SearchResponse getMap(ObservationQuery query, int precision) {

        SearchRequestBuilder searchRequest = createQuery(query);
        GeoHashGridBuilder geohashAggregation = AggregationBuilders.geohashGrid(
                ObservationsFields.AGGREGAT_GEOGRAPHIQUE).precision(precision).field(ObservationsFields.COORDINATES);

        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter
            GeoBoundingBoxFilterBuilder geoFilter = FilterBuilders.geoBoundingBoxFilter(ObservationsFields.COORDINATES)
                    .bottomLeft(query.getFrom().geohash()).topRight(query.getTo().geohash());
            searchRequest.addAggregation(AggregationBuilders.filter(ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE)
                    .subAggregation(geohashAggregation).filter(geoFilter));
        } else {
            searchRequest.addAggregation(geohashAggregation);
        }

        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        // Get aggregation Map only
        return searchRequest.setSize(0).execute().actionGet();
    }

    /**
     * Return the timeline aggregation for specified request query
     * 
     * @param query search in term, time, and geobox
     * @return timeline aggregation with specified interval {@value #TIME_INTERVAL}
     */
    public SearchResponse getTimeline(ObservationQuery query) {

        SearchRequestBuilder searchRequest = createQuery(query);

        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        DateHistogramBuilder histogram = AggregationBuilders.dateHistogram(ObservationsFields.AGGREGAT_DATE).field(
                ObservationsFields.RESULTTIMESTAMP).interval(TIME_INTERVAL).minDocCount(0).extendedBounds(Config
                        .syntheticTimelineMinDate(), null);

        if (query.getFrom() != null && query.getTo() != null) {
            // Prepare geo filter
            GeoBoundingBoxFilterBuilder geoFilter = FilterBuilders.geoBoundingBoxFilter(ObservationsFields.COORDINATES)
                    .bottomLeft(query.getFrom().geohash()).topRight(query.getTo().geohash());

            searchRequest.addAggregation(AggregationBuilders.filter(ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE)
                    .subAggregation(histogram).filter(geoFilter));
        } else {
            searchRequest.addAggregation(histogram);
        }
        // Get aggregation Time only
        return searchRequest.setFrom(0).setSize(0).execute().actionGet();
    }

    /**
     * This method create a query with the observation query element
     * 
     * @param query observation query
     * @return requestbuilder to complete and execute
     */
    private SearchRequestBuilder createQuery(ObservationQuery query) {
        Client client = nodeManager.getClient();
        // Connect on indice
        SearchRequestBuilder searchRequest = client.prepareSearch(Config.observationsIndex());

        BoolQueryBuilder boolQuery = boolQuery();

        // Add keywords fulltext search
        if (StringUtils.isNotBlank(query.getKeywords())) {
            boolQuery.must(queryStringQuery(query.getKeywords()).analyzer(STANDARD_TOKEN_ANALYZER).defaultOperator(
                    Operator.AND));
        }
        // Add range time search
        if (query.getTimeFrom() != null && query.getTimeTo() != null) {
            boolQuery.must(rangeQuery(ObservationsFields.RESULTTIMESTAMP).from(query.getTimeFrom()).to(query
                    .getTimeTo()));
        }

        FilterBuilder permissionFilter = createFilterPermission();
        if (boolQuery.hasClauses()) {
            searchRequest.setQuery(QueryBuilders.filteredQuery(boolQuery, permissionFilter));
        } else {
            searchRequest.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), permissionFilter));
        }

        return searchRequest;
    }

    /**
     * This method create a filter on data using permissions and current user
     */
    private FilterBuilder createFilterPermission() {
        User currentUser = CurrentUserProvider.get();
        // Is Public
        FilterBuilder publicFilter = FilterBuilders.termFilter("snanny-access-type", PUBLIC_ACCESS_TYPE);

        // If current user exist result will be is public or isAuthor or is shared
        if (currentUser != null) {
            return FilterBuilders.boolFilter().should(
                    /** Should be public */
                    publicFilter,
                    /** Should be author */
                    FilterBuilders.termFilter("snanny-author", currentUser.getLogin()),
                    /** Should be shared with current user */
                    FilterBuilders.termFilter("snanny-access-auth", currentUser.getLogin()));
        }
        return publicFilter;

    }
}
