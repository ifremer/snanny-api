package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

public class ObservationsDB {

	public JsonObject getObservations(String bboxQuery, String fromQuery, String toQuery) throws Exception {
		JsonObject result = JsonObject.empty();
		JsonArray features = JsonArray.empty();

		ViewQuery viewQuery = ViewQuery.from("dev_test", "observations_bbox_corrected")
//				.limit(65)
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

//		ViewResult viewResponse = ConnectionManager.observations_dev.query(viewQuery);
		ViewResult viewResponse = ConnectionManager.observations.query(viewQuery);
		for (ViewRow row : viewResponse.allRows()) {

			JsonObject parsedDoc = (JsonObject) row.value();

//			JsonObject observation = JsonObject.empty().put("author", parsedDoc.getString("author")).put("date", row.key())
//					.put("description", parsedDoc.getString("description")).put("boundedBox", parsedDoc.getObject("boundedBox"));
			
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
