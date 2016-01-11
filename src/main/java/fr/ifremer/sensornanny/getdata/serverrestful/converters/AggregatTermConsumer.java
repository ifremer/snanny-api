package fr.ifremer.sensornanny.getdata.serverrestful.converters;

import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.google.gson.JsonObject;

/**
 * Allow to transform term buckets in json element
 * 
 * @author athorel
 *
 */
public class AggregatTermConsumer extends AbstractAggregatConsumer<Bucket> {

    private static final String KEY_PROPERTY = "key";
    private static final String COUNT_PROPERTY = "count";

    @Override
    protected JsonObject createJSonElement(Bucket t) {
        JsonObject element = new JsonObject();
        element.addProperty(KEY_PROPERTY, t.getKey());
        element.addProperty(COUNT_PROPERTY, t.getDocCount());
        return element;
    }

}
