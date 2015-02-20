package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Logger;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

import fr.ifremer.sensornanny.getdata.serverrestful.OMAttributes;

public class ObservationsDB implements OMAttributes {

	private static final Logger logger = Logger.getLogger(ObservationsDB.class.getName());
	
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

	public long getEndDate(String id) {
		long endDate = 0l;
		JsonObject observation = getObservation(id);
		
		if (observation.containsKey(ATTRIBUTE_FILE_JSON)) {
			JsonObject fileJSON = observation.getObject(ATTRIBUTE_FILE_JSON);
			if (fileJSON.containsKey(ATTRIBUTE_CHILD_NODES)) {
				for (Object each : fileJSON.getArray(ATTRIBUTE_CHILD_NODES)) {
					JsonObject element = (JsonObject) each;
					if (ATTRIBUTE_OM_PHENOMENON_TIME.equals(element.getString(ATTRIBUTE_TAG_NAME))) {
						JsonArray timePeriodArray = ((JsonObject) element.getArray(ATTRIBUTE_CHILD_NODES).get(0)).getArray(ATTRIBUTE_CHILD_NODES);
						for (Object each2 : timePeriodArray) {
							JsonObject element2 = (JsonObject) each2;
							if (ATTRIBUTE_GML_END_POSITION.equals(element2.getString(ATTRIBUTE_TAG_NAME)) && element2.containsKey(ATTRIBUTE_CHILD_NODES)) {
								final String date = element2.getArray(ATTRIBUTE_CHILD_NODES).getString(0);
								try {
									endDate = new SimpleDateFormat(DATE_FORMAT).parse(date).getTime();
								} catch (ParseException e) {
									logger.severe("Unable to parse date " + date + " from observation with id = " + id);
								}
							}
						}
					}
				}
			}
		}
		
		return endDate;
	}

	public JsonObject getObservation(String id) {
		JsonDocument ret = ConnectionManager.observations.get(id);
		return ret.content();
	}
	
	public String getResultPath(String id) {
		String resultPath = null;
		JsonObject observation = getObservation(id);
		
		if (observation.containsKey(ATTRIBUTE_FILE_JSON)) {
			JsonObject fileJSON = observation.getObject(ATTRIBUTE_FILE_JSON);
			if (fileJSON.containsKey(ATTRIBUTE_CHILD_NODES)) {
				for (Object each : fileJSON.getArray(ATTRIBUTE_CHILD_NODES)) {
					JsonObject element = (JsonObject) each;
					if (ATTRIBUTE_OM_RESULT.equals(element.getString(ATTRIBUTE_TAG_NAME))) {
						JsonArray array = element.getArray(ATTRIBUTE_CHILD_NODES);
						if (array != null && array.size() > 0) {
							JsonArray resultDataArray = ((JsonObject) array.get(0)).getArray(ATTRIBUTE_CHILD_NODES);
							if (resultDataArray != null) {
								for (Object each2 : resultDataArray) {
									JsonObject element2 = (JsonObject) each2;
									if (ATTRIBUTE_SWE_VALUES.equals(element2.getString(ATTRIBUTE_TAG_NAME)) && element2.containsKey(ATTRIBUTE_XLINK_HREF)) {
										resultPath = element2.getString(ATTRIBUTE_XLINK_HREF);
										break;
									}
								}
							} else {
								for (Object each2 : array) {
									JsonObject element2 = (JsonObject) each2;
									if (ATTRIBUTE_SWE_VALUES.equals(element2.getString(ATTRIBUTE_TAG_NAME)) && element2.containsKey(ATTRIBUTE_XLINK_HREF)) {
										resultPath = element2.getString(ATTRIBUTE_XLINK_HREF);
										break;
									}
								}
							}
						}
						break;
					}
				}
			}
		}
		
		return resultPath;
	}

}
