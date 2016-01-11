package fr.ifremer.sensornanny.getdata.serverrestful.util.query;

import org.elasticsearch.common.geo.GeoPoint;

import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;

/**
 * This class allow to resolve queryString and transform it to query parameters
 * 
 * @author athorel
 *
 */
public final class QueryResolver {

    /**
     * This method allow to resolve query parameter and ensure that the query
     * is well-formed
     * 
     * @param bboxQuery bounds parameter of the map (viewable zone)
     * @param timeQuery range parameter, allow search between two dates
     * @param keywordsQuery keywords for fulltext search
     * @return queryObject with parameters
     */
    public static ObservationQuery resolveQueryObservation(String bboxQuery, String timeQuery, String keywordsQuery) {

        ObservationQuery result = new ObservationQuery();
        if (bboxQuery != null) {
            String[] bbox = bboxQuery.split(",");
            if (bbox.length == 4) {
                double queryLowerLatitude = Double.parseDouble(bbox[0]);
                double queryLowerLongitude = Double.parseDouble(bbox[1]);
                double queryUpperLatitude = Double.parseDouble(bbox[2]);
                double queryUpperLongitude = Double.parseDouble(bbox[3]);

                if (queryLowerLatitude < -90)
                    queryLowerLatitude = -90;
                if (queryLowerLongitude < -180)
                    queryLowerLongitude = -180;
                if (queryUpperLatitude > 90)
                    queryUpperLatitude = 90;
                if (queryUpperLongitude > 180)
                    queryUpperLongitude = 180;

                result.setFrom(new GeoPoint(queryLowerLatitude, queryLowerLongitude));
                result.setTo(new GeoPoint(queryUpperLatitude, queryUpperLongitude));
            }
        }

        if (timeQuery != null) {
            try {
                String[] time = timeQuery.split(",");
                if (time.length == 2) {
                    result.setTimeFrom(Long.parseLong(time[0]));
                    result.setTimeTo(Long.parseLong(time[1]));
                }
            } catch (NumberFormatException e) {
                // Impossible to get time query -> No filter
            }
        }

        result.setKeywords(keywordsQuery);

        return result;
    }
}
