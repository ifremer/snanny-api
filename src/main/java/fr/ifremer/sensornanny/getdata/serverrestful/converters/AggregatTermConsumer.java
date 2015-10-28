package fr.ifremer.sensornanny.getdata.serverrestful.converters;

import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.couchbase.client.java.document.json.JsonObject;

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
        JsonObject element = JsonObject.create();
        element.put(KEY_PROPERTY, t.getKey());
        element.put(COUNT_PROPERTY, t.getDocCount());
        return element;
    }

}
