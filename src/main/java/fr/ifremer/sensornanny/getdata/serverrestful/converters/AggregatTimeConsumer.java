package fr.ifremer.sensornanny.getdata.serverrestful.converters;

import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;

import com.google.gson.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;

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
    private static final long SEMI_PERIOD_INTERVAL = (Config.syntheticViewTimeSize() * 24 * 60 * 60 * 1000) / 2;

    @Override
    protected JsonObject createJSonElement(Bucket t) {
        long longValue = t.getKeyAsNumber().longValue();
        if (longValue < 0) {
            return null;
        }
        JsonObject element = new JsonObject();
        long timeInMillis = longValue;
        element.addProperty(EVENT_PROPERTY, timeInMillis);
        element.addProperty(VALUE_PROPERTY, t.getDocCount());

        JsonObject time = new JsonObject();
        time.addProperty(BEGIN_PROPERTY, (timeInMillis - SEMI_PERIOD_INTERVAL));
        time.addProperty(END_PROPERTY, (timeInMillis + SEMI_PERIOD_INTERVAL));
        element.add(TIME_PROPERTY, time);

        return element;
    }

}
