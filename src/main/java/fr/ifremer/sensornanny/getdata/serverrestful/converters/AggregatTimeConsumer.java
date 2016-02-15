package fr.ifremer.sensornanny.getdata.serverrestful.converters;

import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;

import com.google.gson.JsonArray;
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

    private long startTime = Long.MAX_VALUE;
    private long endTime = Long.MIN_VALUE;

    @Override
    protected JsonObject createJSonElement(Bucket t) {

        long longValue = ((org.joda.time.DateTime) t.getKey()).getMillis();
        if (longValue < 0) {
            return null;
        }

        long docCount = t.getDocCount();
        JsonObject element = createTimeElement(longValue, docCount);

        return element;
    }

    private JsonObject createTimeElement(long timeInMillis, long docCount) {

        JsonObject time = new JsonObject();
        JsonObject element = new JsonObject();
        element.addProperty(EVENT_PROPERTY, timeInMillis);
        element.addProperty(VALUE_PROPERTY, docCount);
        long beginProp = timeInMillis - SEMI_PERIOD_INTERVAL;
        long endProp = timeInMillis + SEMI_PERIOD_INTERVAL;

        time.addProperty(BEGIN_PROPERTY, beginProp);
        time.addProperty(END_PROPERTY, endProp);

        if (beginProp < startTime) {
            startTime = beginProp;
        }
        if (endProp > endTime) {
            endTime = endProp;
        }
        element.add(TIME_PROPERTY, time);
        return element;
    }

    @Override
    public JsonArray getResult() {
        JsonArray result = super.getResult();
        long tenPercentSpace = 0;
        if (result.size() == 0) {
            return result;
        }

        // Calc size of new array
        if (result.size() == 1) {
            // IF only one bucklet add one interval between each
            tenPercentSpace = SEMI_PERIOD_INTERVAL;
        } else {
            // Ten percent (and get middle)
            tenPercentSpace = (endTime - startTime) / 20;
        }
        JsonArray newArr = new JsonArray();
        newArr.add(createTimeElement(startTime - tenPercentSpace, 0));
        newArr.addAll(result);
        newArr.add(createTimeElement(endTime + tenPercentSpace, 0));

        return newArr;
    }

}
