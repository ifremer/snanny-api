package fr.ifremer.sensornanny.getdata.serverrestful.io.elastic;

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
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;

/**
 * This class allow access to elasticsearch observations databases
 * 
 * @author athorel
 *
 */
public class ObservationsSearch {

    private static final int MILLIS_TO_SECONDS = 1000;

    private static final int TIME_INTERVAL = 15 * 24 * 60 * 60;

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
        searchRequest.setFetchSource("doc.*", "meta.*");
        searchRequest.setScroll(ElasticConfiguration.scrollTimeout());
        return searchRequest.setFrom(0).setSize(ElasticConfiguration.scrollPagination()).execute().actionGet(
                ElasticConfiguration.queryTimeout(), TimeUnit.MILLISECONDS);
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
        return client.prepareSearchScroll(scrollId).setScroll(ElasticConfiguration.scrollTimeout()).execute().actionGet(
                ElasticConfiguration.queryTimeout(), TimeUnit.MILLISECONDS);
    }

    /**
     * Get GeoJSON object representing ths aggregations for specific observation query
     * 
     * @param query search in term, time, and geobox
     * 
     * @return aggregations of documents limited by the query in zones
     */
    public SearchResponse getMap(ObservationQuery query) {

        SearchRequestBuilder searchRequest = createQuery(query);

        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter
            GeoBoundingBoxFilterBuilder geoFilter = FilterBuilders.geoBoundingBoxFilter(ObservationsFields.COORDINATES)
                    .bottomLeft(query.getFrom().geohash()).topRight(query.getTo().geohash());

            searchRequest.addAggregation(AggregationBuilders.filter(ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE)
                    .subAggregation(AggregationBuilders.geohashGrid(ObservationsFields.AGGREGAT_GEOGRAPHIQUE).precision(
                            4).field(ObservationsFields.COORDINATES)).filter(geoFilter));
        } else {
            searchRequest.addAggregation(AggregationBuilders.geohashGrid(ObservationsFields.AGGREGAT_GEOGRAPHIQUE)
                    .precision(4).field(ObservationsFields.COORDINATES));

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

        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter
            GeoBoundingBoxFilterBuilder geoFilter = FilterBuilders.geoBoundingBoxFilter(ObservationsFields.COORDINATES)
                    .bottomLeft(query.getFrom().geohash()).topRight(query.getTo().geohash());

            searchRequest.addAggregation(AggregationBuilders.filter(ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE)
                    .subAggregation(AggregationBuilders.histogram(ObservationsFields.AGGREGAT_DATE).field(
                            ObservationsFields.RESULTTIMESTAMP).interval(TIME_INTERVAL)).filter(geoFilter));
        } else {
            searchRequest.addAggregation(AggregationBuilders.histogram(ObservationsFields.AGGREGAT_DATE).field(
                    ObservationsFields.RESULTTIMESTAMP).interval(TIME_INTERVAL));

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
        SearchRequestBuilder searchRequest = client.prepareSearch(ElasticConfiguration.observationsIndex());

        BoolQueryBuilder boolQuery = boolQuery();
        // Add keywords fulltext search
        if (StringUtils.isNotBlank(query.getKeywords())) {
            boolQuery.must(queryStringQuery(query.getKeywords()));
        }
        // Add range time search
        if (query.getTimeFrom() != null && query.getTimeTo() != null) {
            boolQuery.must(rangeQuery(ObservationsFields.RESULTTIMESTAMP).from(query.getTimeFrom() / MILLIS_TO_SECONDS)
                    .to(query.getTimeTo() / MILLIS_TO_SECONDS));
        }

        if (boolQuery.hasClauses()) {
            searchRequest.setQuery(boolQuery);
        }

        return searchRequest;
    }
}
