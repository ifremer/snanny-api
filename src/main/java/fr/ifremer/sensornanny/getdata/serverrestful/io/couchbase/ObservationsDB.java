package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

public class ObservationsDB {

	public JsonObject getObservations(String bboxQuery, String fromQuery, String toQuery) {
		JsonObject result = JsonObject.empty();
		JsonArray features = JsonArray.empty();

		ViewQuery viewQuery = ViewQuery.from(Configuration.getInstance().observationsViewDesign(), Configuration.getInstance().observationsViewName())
				;

		if (!fromQuery.isEmpty() && !toQuery.isEmpty()) {
			viewQuery.startKey(fromQuery);
			viewQuery.endKey(toQuery);
		} else if (!fromQuery.isEmpty()) {
			viewQuery.startKey(fromQuery);
		} else if (!toQuery.isEmpty()) {
			viewQuery.endKey(toQuery);
		}

		if (!bboxQuery.isEmpty()) {
			// TODO

		}

		ViewResult viewResponse = ConnectionManager.observations.query(viewQuery);
		for (ViewRow row : viewResponse.allRows()) {

			JsonObject parsedDoc = (JsonObject) row.value();

			parsedDoc.getObject("properties").put("id", row.id());

			features.add(parsedDoc);
		}
		
		result.put("type", "FeatureCollection");
		result.put("features", features);

		return result;
	}

	public JsonObject getObservation(String id) {
		JsonDocument ret = ConnectionManager.observations.get(id);
		return ret.content();
	}

}
