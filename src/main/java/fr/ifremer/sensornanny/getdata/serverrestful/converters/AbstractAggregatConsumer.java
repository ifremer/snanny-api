package fr.ifremer.sensornanny.getdata.serverrestful.converters;

import java.util.function.Consumer;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Abstract class that allow to create JSON array of of aggregat result
 * 
 * @author athorel
 *
 * @param <T>
 *            Type of bucket
 */
public abstract class AbstractAggregatConsumer<T> implements Consumer<T> {

    /**
     * Result array
     */
    protected JsonArray jsonArray = JsonArray.create();

    /**
     * Get the json array result
     * 
     * @return result array
     */
    public JsonArray getResult() {
        return jsonArray;
    }

    @Override
    public void accept(T t) {
        JsonObject jsonElement = createJSonElement(t);

        if (jsonElement != null) {
            jsonArray.add(jsonElement);
        }
    }

    /**
     * Abstract method that allow to create json element from a bucket
     * 
     * @param t
     *            bucket element
     * @return json description of the bucket
     */
    protected abstract JsonObject createJSonElement(T t);
}
