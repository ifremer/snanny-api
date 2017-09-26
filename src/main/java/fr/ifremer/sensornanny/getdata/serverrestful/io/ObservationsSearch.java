package fr.ifremer.sensornanny.getdata.serverrestful.io;

import static fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields.*;
import static fr.ifremer.sensornanny.getdata.serverrestful.constants.SystemFields.SYSTEMS_HASDATA;
import static org.elasticsearch.index.query.QueryBuilders.*;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;
import fr.ifremer.sensornanny.getdata.serverrestful.constants.ObservationsFields;
import fr.ifremer.sensornanny.getdata.serverrestful.context.CurrentUserProvider;
import fr.ifremer.sensornanny.getdata.serverrestful.context.Role;
import fr.ifremer.sensornanny.getdata.serverrestful.context.User;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;

/**
 * This class allow access to elasticsearch observations databases
 *
 * @author athorel
 */
public class ObservationsSearch {

    private static final Log LOGGER = LogFactory.getLog(ObservationsSearch.class);

    private static final String SNANNY_SHARE_AUTH = "doc.snanny-access.snanny-access-auth";
    private static final String SNANNY_AUTHOR = "doc.snanny-author";
    private static final String SNANNY_ACCESS = "doc.snanny-access.snanny-access-type";

    private static final String EMPTY_STRING = "";

    private static final int PUBLIC_ACCESS_TYPE = 2;

    private static final String[] EXCLUDE_OBSERVATION_FIELDS = new String[]{"meta.*"};

    private static final String[] EXPORT_OBSERVATIONS_FIELDS = new String[]{"snanny-uuid", "snanny-deploymentid",
            "snanny-ancestors.snanny-ancestor-name", "snanny-ancestors.snanny-ancestor-uuid",
            "snanny-ancestors.snanny-ancestor-deploymentid", "snanny-name", "snanny-coordinates"};

    private static final String STANDARD_TOKEN_ANALYZER = "standard";

    private static final int DAYS_IN_MILLIS = 24 * 60 * 60 * 1000;

    private NodeManager nodeManager = new NodeManager();

    /**
     * Get a page of observations from a query
     *
     * @param query geoboxing, timeboxing and keywords parameters
     * @return response containing the first page of observations using #ElasticConfiguration.scrollPagination()
     */
    public SearchResponse getObservations(ObservationQuery query, boolean hasCoords) {

        SearchRequestBuilder searchRequest = createQuery(query);

        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter        	          
            QueryBuilder geoFilter = QueryBuilders.geoBoundingBoxQuery(ObservationsFields.COORDINATES)
            		.setCornersOGC(query.getFrom(),query.getTo());
            
            // Add Range data
            searchRequest.setPostFilter(geoFilter);
        }
        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        searchRequest.setFetchSource(EXPORT_OBSERVATIONS_FIELDS, EXCLUDE_OBSERVATION_FIELDS);
        if(!hasCoords) {
            QueryBuilder filterBuilder = new BoolQueryBuilder().mustNot(
                    new ExistsQueryBuilder(ObservationsFields.COORDINATES)
            );
            searchRequest.setPostFilter(filterBuilder);
        }

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
     * @return aggregations of documents limited by the query in zones
     */
    public SearchResponse getMap(ObservationQuery query, int precision) {

        SearchRequestBuilder searchRequest = createQuery(query);
        
        GeoGridAggregationBuilder geohashAggregation = AggregationBuilders.geohashGrid(
                ObservationsFields.AGGREGAT_GEOGRAPHIQUE).precision(precision).field(ObservationsFields.COORDINATES);
        
        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter
            QueryBuilder geoFilter = QueryBuilders.geoBoundingBoxQuery(ObservationsFields.COORDINATES)
            		.setCornersOGC(query.getFrom(),query.getTo());

            searchRequest.addAggregation(AggregationBuilders.filter(ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE, geoFilter)
            		.subAggregation(geohashAggregation)); 
            
        } else {
            searchRequest.addAggregation(geohashAggregation);
        }

        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(searchRequest);
        }

        // Get aggregation Map only
        return searchRequest.setSize(0).execute().actionGet();
    }

    /**
     * Return the timeline aggregation for specified request query
     *
     * @param query search in term, time, and geobox
     * @return timeline aggregation with specified interval {@link Config#syntheticViewTimeSize()} *
     * {@value #DAYS_IN_MILLIS}
     */
    public SearchResponse getTimeline(ObservationQuery query, Boolean hasCoords) {

        SearchRequestBuilder searchRequest = createQuery(query);

        if(hasCoords == null || (hasCoords != null && hasCoords)){
            searchRequest.setQuery(QueryBuilders.existsQuery(ObservationsFields.COORDINATES));
        }

        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        
        long interval = Config.syntheticViewTimeSize() * DAYS_IN_MILLIS;
        
        HistogramAggregationBuilder histogram = AggregationBuilders.histogram(ObservationsFields.AGGREGAT_DATE)
                .field(ObservationsFields.RESULTTIMESTAMP)
                .interval(interval).minDocCount(0);

        if (query.getFrom() != null && query.getTo() != null) {
            // Prepare geo filter
            QueryBuilder geoFilter = QueryBuilders.geoBoundingBoxQuery(ObservationsFields.COORDINATES)
            		.setCornersOGC(query.getFrom(),query.getTo());
            
            searchRequest.addAggregation(AggregationBuilders.filter(ObservationsFields.ZOOM_IN_AGGREGAT_GEOGRAPHIQUE, geoFilter)
            		.subAggregation(histogram));
            
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
        String keywords = query.getKeywords();
        if (keywords != null && !EMPTY_STRING.equals(keywords.trim())) {
            boolQuery.must(queryStringQuery(keywords).analyzer(STANDARD_TOKEN_ANALYZER).defaultOperator(Operator.AND));
        }
        // Add range time search
        if (query.getTimeFrom() != null && query.getTimeTo() != null) {
            boolQuery.must(rangeQuery(ObservationsFields.RESULTTIMESTAMP).from(query.getTimeFrom()).to(query
                    .getTimeTo()));
        }

        // If the user filter is enabled filter the result otherwise only execute request
        if (Config.userFilterEnabled()) {
            QueryBuilder permissionFilter = createFilterPermission();
            if (boolQuery.hasClauses()) {
                searchRequest.setQuery(QueryBuilders.boolQuery().must(boolQuery).filter(permissionFilter));
            } else {
                searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(
                        permissionFilter));
            }
        } else if (boolQuery.hasClauses()) {
            searchRequest.setQuery(boolQuery);
        }

        return searchRequest;
    }

    /**
     * This method create a filter on data using permissions and current user
     */
    private QueryBuilder createFilterPermission() {
        User currentUser = CurrentUserProvider.get();
        // Is Public
        QueryBuilder publicFilter = QueryBuilders.termQuery(SNANNY_ACCESS, PUBLIC_ACCESS_TYPE);

        // If current user exist result will be is public or isAuthor or is shared
        if (currentUser != null) {
            if (!Role.ADMIN.equals(currentUser.getRole())) {
                return QueryBuilders.boolQuery()
                        /** Should be public */
                        .should(publicFilter)
                                /** Should be author */
                        .should(QueryBuilders.termQuery(SNANNY_AUTHOR, currentUser.getLogin()))
                                /** Should be shared with current user */
                        .should(QueryBuilders.termQuery(SNANNY_SHARE_AUTH, currentUser.getLogin()));
            } else if (Role.ADMIN.equals(currentUser.getRole())) {
                return QueryBuilders.matchAllQuery();
            }
        }
        return QueryBuilders.boolQuery().should(publicFilter);

    }

    public SearchResponse getSystems(ObservationQuery query) {

        SearchRequestBuilder searchRequest = createQuery(query);

        //TermsBuilder terms = AggregationBuilders.terms(AGGREGAT_TERM).field(SNANNY_ANCESTORS + "." + SNANNY_ANCESTOR_UUID);
        TermsAggregationBuilder terms = AggregationBuilders.terms(AGGREGAT_TERM).field(SNANNY_ANCESTORS + "." + SNANNY_ANCESTOR_UUID);
        addAggregationsInTerm(terms, SNANNY_ANCESTOR_NAME, SNANNY_ANCESTOR_DEPLOYMENTID, SNANNY_ANCESTOR_DESCRIPTION, SNANNY_ANCESTOR_TERMS);

        // NestedBuilder nested = AggregationBuilders.nested(AGGREGAT).path(SNANNY_ANCESTORS).subAggregation(terms);
        NestedAggregationBuilder nested = AggregationBuilders.nested(AGGREGAT, SNANNY_ANCESTORS).subAggregation(terms);
        
        if (query.getFrom() != null && query.getTo() != null) {

            // Prepare geo filter
            QueryBuilder geoFilter = QueryBuilders.geoBoundingBoxQuery(COORDINATES)
            		.setCornersOGC(query.getFrom(),query.getTo());

            // Add Range data
            searchRequest.setPostFilter(geoFilter);
        }

        searchRequest.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setFetchSource(EXPORT_OBSERVATIONS_FIELDS, EXCLUDE_OBSERVATION_FIELDS)
                .addAggregation(nested).setSize(0);

        return searchRequest.execute().actionGet();
    }

    public SearchResponse getSystemsByField(String id, String field) {
        SearchRequestBuilder requestBuilder = nodeManager.getClient().prepareSearch(Config.observationsIndex());

        requestBuilder.setQuery(QueryBuilders.termQuery(ObservationsFields.SNANNY_ANCESTORS, 
        		QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(field, id))));
        
        requestBuilder.setSize(1);

        return requestBuilder.execute().actionGet();
    }

    public SearchResponse getSystemsWithData(String hasData) {
        SearchRequestBuilder requestBuilder = nodeManager.getClient().prepareSearch(Config.systemsIndex());

        if(hasData == null){
            requestBuilder.setQuery(QueryBuilders.matchAllQuery());
        } else {
            Boolean withData = Boolean.parseBoolean(hasData);
            requestBuilder.setQuery(QueryBuilders.matchQuery(SYSTEMS_HASDATA, withData));
        }

        return requestBuilder.execute().actionGet();
    }

    private void addAggregationsInTerm(AggregationBuilder aggBuilder, String... aggregationsName) {
        for(String aggName : aggregationsName) {
            AggregationBuilder agg = AggregationBuilders.terms(aggName).field(SNANNY_ANCESTORS + "." + aggName);
            aggBuilder.subAggregation(agg);
        }
    }

}
