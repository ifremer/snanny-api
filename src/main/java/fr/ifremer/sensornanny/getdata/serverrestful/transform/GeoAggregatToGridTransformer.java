package fr.ifremer.sensornanny.getdata.serverrestful.transform;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid.Bucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import fr.ifremer.sensornanny.getdata.serverrestful.dto.DegreesDivision;
import fr.ifremer.sensornanny.getdata.serverrestful.util.math.MathUtil;

/**
 * Class that allow to transform aggregat to grid
 * 
 * @author athorel
 *
 */
public class GeoAggregatToGridTransformer {

    private static final String GEOMETRY_PROPERTY = "geometry";
    private static final String COORDINATES_PROPERTY = "coordinates";
    private static final String POLYGON_VALUE = "Polygon";
    private static final String PROPERTIES_PROPERTY = "properties";
    private static final String RATIO_PROPERTY = "ratio";
    private static final String COUNT_PROPERTY = "count";
    private static final String FEATURE_VALUE = "Feature";
    private static final String TYPE_PROPERTY = "type";
    private static final Logger LOGGER = Logger.getLogger(GeoAggregatToGridTransformer.class.getName());

    /**
     * Transform an aggregat to a map of values
     * 
     * @param aggregat aggregat to transform
     * @param degreesDivision division degree
     * @return map with <code>key</code> contains the base degree and
     *         <code>value</code> contains the total count of item
     */
    private static Map<DegreesDivision, Long> transform(InternalGeoHashGrid aggregat, final double degreesDivision) {

        LOGGER.info("create a grid from aggregat with degrees division : " + degreesDivision);

        final Map<DegreesDivision, Long> result = new HashMap<>();
        aggregat.getBuckets().forEach(new Consumer<GeoHashGrid.Bucket>() {

            @Override
            public void accept(Bucket t) {
                GeoPoint geoPoint = t.getKeyAsGeoPoint();

                // Calculate nearest geopoint
                double lat = geoPoint.getLat() + 90;
                double lon = geoPoint.getLon() + 180;

                lat = Math.floor(lat * 10) / 10;
                lon = Math.floor(lon * 10) / 10;

                lat = (lat - (lat % degreesDivision)) - 90;
                lon = (lon - (lon % degreesDivision)) - 180;

                lat = MathUtil.floorTwoDigits(lat);
                lon = MathUtil.floorTwoDigits(lon);

                DegreesDivision division = new DegreesDivision(lat, lon);
                if (result.containsKey(division)) {
                    result.put(division, result.get(division) + t.getDocCount());
                } else {
                    result.put(division, t.getDocCount());
                }
            }
        });
        return result;
    }

    /**
     * Transform an aggregat to a map of values
     * 
     * @param aggregat aggregat to transform
     * 
     * @param degreesDivision division degree
     * @param totalCount number of documents raised by the query
     * @param totalVisible number of documents raised by the query which are visible in
     *            the geobox
     * 
     * @return GeoJsonObject with polygone shape and totalitems
     */
    public static JsonArray toGeoJson(InternalGeoHashGrid aggregat, double degreesDivision, long totalCount,
            long totalVisible) {

        LOGGER.info("create a GeoJson element from grid");

        Map<DegreesDivision, Long> map = transform(aggregat, degreesDivision);

        JsonArray resultArray = new JsonArray();
        if (!map.isEmpty()) {
            Set<java.util.Map.Entry<DegreesDivision, Long>> entrySet = map.entrySet();
            final Long maxValue = Collections.max(map.values());
            for (java.util.Map.Entry<DegreesDivision, Long> each : entrySet) {

                JsonObject feature = new JsonObject();
                feature.addProperty(TYPE_PROPERTY, FEATURE_VALUE);

                JsonObject properties = new JsonObject();
                float floatValue = (float) each.getValue();
                properties.addProperty(COUNT_PROPERTY, floatValue);
                properties.addProperty(RATIO_PROPERTY, (int) 0.25f + (floatValue * 75 / maxValue));
                feature.add(PROPERTIES_PROPERTY, properties);

                JsonObject geometry = new JsonObject();
                geometry.addProperty(TYPE_PROPERTY, POLYGON_VALUE);
                JsonArray coordinates = new JsonArray();

                JsonArray coordinate = new JsonArray();
                coordinates.add(coordinate);

                double lowerLatitude = each.getKey().getLat();
                double lowerLongitude = each.getKey().getLon();
                double upperLatitude = MathUtil.floorTwoDigits(lowerLatitude + degreesDivision);
                double upperLongitude = MathUtil.floorTwoDigits(lowerLongitude + degreesDivision);

                coordinate.add(createPoint(lowerLongitude, upperLatitude));
                coordinate.add(createPoint(upperLongitude, upperLatitude));
                coordinate.add(createPoint(upperLongitude, lowerLatitude));
                coordinate.add(createPoint(lowerLongitude, lowerLatitude));
                coordinate.add(createPoint(lowerLongitude, upperLatitude));

                geometry.add(COORDINATES_PROPERTY, coordinates);
                feature.add(GEOMETRY_PROPERTY, geometry);

                resultArray.add(feature);
            }
        }

        return resultArray;
    }

    private static JsonArray createPoint(double longiture, double latitude) {
        JsonArray arr = new JsonArray();
        arr.add(new JsonPrimitive(longiture));
        arr.add(new JsonPrimitive(latitude));
        return arr;
    }

}
