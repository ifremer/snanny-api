package fr.ifremer.sensornanny.getdata.serverrestful.converters;

import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * Classe allow to transform Historigram buckets in json object
 * 
 * @author athorel
 *
 */
public class AggregatTimeConsumer extends AbstractAggregatConsumer<InternalHistogram.Bucket> {

    private static final String VALUE_PROPERTY = "value";
    private static final String EVENT_PROPERTY = "event";
    private static final String TIME_PROPERTY = "time";
    private static final String BEGIN_PROPERTY = "begin";
    private static final String END_PROPERTY = "end";
    private static final long SEMI_PERIOD_INTERVAL = (15 * 24 * 60 * 60 * 1000) / 2;

    @Override
    protected JsonObject createJSonElement(Bucket t) {
        long longValue = t.getKeyAsNumber().longValue();
        if (longValue < 0) {
            return null;
        }
        JsonObject element = JsonObject.create();
        long timeInMillis = longValue;
        element.put(EVENT_PROPERTY, timeInMillis);
        element.put(VALUE_PROPERTY, t.getDocCount());

        JsonObject time = JsonObject.create();
        time.put(BEGIN_PROPERTY, (timeInMillis - SEMI_PERIOD_INTERVAL));
        time.put(END_PROPERTY, (timeInMillis + SEMI_PERIOD_INTERVAL));
        element.put(TIME_PROPERTY, time);

        return element;
    }

}
